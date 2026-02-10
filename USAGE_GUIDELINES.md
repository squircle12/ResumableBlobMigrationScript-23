# Blob Delta Loads – Usage Guidelines

This document provides practical usage guidelines, FAQ, backup/transfer procedures, and supplier workflow for the Blob Delta Jobs (V3) system.

---

## 1. How to Use

### 1.1 Initial Setup (One-Time)

1. **Deploy the database objects** (in order, once per environment):
   - Run `03_BlobDeltaJobs_Schema.sql` – creates `BlobDeltaJobs` database and core tables.
   - Run `04_BlobDeltaJobs_Seed_Config.sql` – seeds table config and script templates.
   - Run `05_BlobDeltaJobs_Engine.sql` – deploys the engine procedures.

2. **Verify configuration** in `BlobDeltaTableConfig`:
   - Tables and databases (e.g. `ReferralAttachment`, `ClientAttachment` in `Gwent_LA_FileTable`—actual values depend on your setup).
   - Confirm `SafetyBufferMinutes` (default 240).
   - Confirm source/target/metadata database names match your environment.

3. **Optionally schedule** via SQL Server Agent:
   - Create a weekly (or other cadence) job that calls `usp_BlobDelta_RunOperator` (see below).

### 1.2 Running Delta Loads

| Scenario | Command |
|----------|---------|
| **All tables, all BUs (typical scheduled run)** | `EXEC dbo.usp_BlobDelta_RunOperator @Mode = N'AllTables', @TableName = NULL, @BatchSize = 500, @MaxDOP = 2, @BusinessUnitId = NULL` |
| **Single table (manual catch-up / testing)** | `EXEC dbo.usp_BlobDelta_RunOperator @Mode = N'SingleTable', @TableName = N'<TargetDB>.dbo.<TableName>', @BatchSize = 500, @MaxDOP = 2, @BusinessUnitId = NULL` (e.g. `Gwent_LA_FileTable.dbo.ReferralAttachment`) |
| **Specific business unit only** | `EXEC dbo.usp_BlobDelta_RunOperator @Mode = N'AllTables', @TableName = NULL, @BatchSize = 500, @MaxDOP = 2, @BusinessUnitId = '<GUID>'` |

- Always run from the `BlobDeltaJobs` database.
- The engine populates the **target** tables defined in `BlobDeltaTableConfig` (for example, `Gwent_LA_FileTable.dbo.ReferralAttachment` and `ClientAttachment`—actual tables depend on your config) with new and updated blob records based on `[ModifiedOn]`.

---

## 2. Frequently Asked Questions

### General

**Q: How often should I run the delta job?**  
A: Typically weekly, or according to your change volume. The engine uses `ModifiedOn` windows and high-watermarks, so it is safe to run on a schedule. Overlap between runs is handled by the safety buffer; consumers apply “latest per blob” logic.

**Q: Can I run multiple delta jobs at the same time?**  
A: No. The engine uses a lease (`IsRunning`, `RunLeaseExpiresAt`) per table to prevent overlapping runs. If a run fails, the lease is cleared so a new run can proceed.

**Q: What if a run fails partway through?**  
A: High-watermarks are only advanced when all three steps (Roots, Missing Parents, Children) succeed for a table. On failure, the lease is cleared. You can re-run; the next run will reprocess from the last successful high-watermark.

### Data and Behaviour

**Q: Why might the same blob appear in more than one delta?**  
A: The safety buffer overlaps windows, and frequently updated records may appear in consecutive runs. This is intentional. Consumers should use “latest by `ModifiedOn`” semantics: if a `stream_id` exists in multiple deltas, keep the row with the latest `ModifiedOn`.

**Q: How are deletions handled?**  
A: Deletions are not managed at present. The delta pipeline does not propagate deletes.

**Q: What is the safety buffer and should I change it?**  
A: The default 4-hour buffer reduces the risk of missing late writes or clock drift. Adjust `SafetyBufferMinutes` in `BlobDeltaTableConfig` only if you have a strong reason (e.g. very large buffer for cross-timezone issues).

### Operations

**Q: How do I add a new table to delta loads?**  
A: Insert a row into `BlobDeltaTableConfig` with source/target/metadata details and `MetadataModifiedOnCol`, then add a matching row in `BlobDeltaHighWatermark`. The existing script templates may work; otherwise add/update scripts in `BlobDeltaStepScript` and `BlobDeltaQueuePopulationScript`.

**Q: How do I see what was processed in a run?**  
A: Use `BlobDeltaRun` and `BlobDeltaRunStep` (see [Monitoring](#3-monitoring-and-troubleshooting)). You can correlate rows with the target tables using `stream_id` and `ModifiedOn`.

---

## 3. Monitoring and Troubleshooting

### Recent runs

```sql
SELECT TOP (50)
    RunId, RunType, RequestedBy, RunStartedAt, RunCompletedAt, Status, ErrorMessage
FROM BlobDeltaJobs.dbo.BlobDeltaRun
ORDER BY RunStartedAt DESC;
```

### Per-step progress for a run

```sql
SELECT TableName, StepNumber, BatchNumber, RowsProcessed, TotalRowsProcessed,
       WindowStart, WindowEnd, BatchStartedAt, BatchCompletedAt, Status, ErrorMessage
FROM BlobDeltaJobs.dbo.BlobDeltaRunStep
WHERE RunId = @RunId
ORDER BY TableName, StepNumber, BatchNumber;
```

### High-watermarks

```sql
SELECT TableName, LastHighWaterModifiedOn, LastRunId, LastRunCompletedAt,
       IsInitialFullLoadDone, IsRunning, RunLeaseExpiresAt
FROM BlobDeltaJobs.dbo.BlobDeltaHighWatermark
ORDER BY TableName;
```

---

## 4. Backup and Transfer to Supplier

The delta engine writes to **target** tables in FileTable databases. The system runs across **multiple databases**; each table in `BlobDeltaTableConfig` defines its own source, target, and metadata databases. For example, `Gwent_LA_FileTable.dbo.ReferralAttachment` and `Gwent_LA_FileTable.dbo.ClientAttachment` are illustrative—your actual target databases and tables depend on your configuration.

### 4.1 Backup Format and Scope

- **Format:** Native SQL Server `.BAK` (full backup).
- **Scope:** Full database backup for each target FileTable database. Each configured target database should be backed up in full after the delta run.
- **Content:** The backup includes full filestream data. Target tables such as `ClientAttachment` and `ReferralAttachment` are FileTable tables; their schema defines both metadata and filestream columns. A full database backup captures the complete FileTable data (metadata plus file content).

### 4.2 Transfer and Notification

1. Place the `.bak` file(s) in a staging location after each delta run (or on schedule).
2. Transfer the backup file(s) to the supplier using the agreed secure file transfer process (e.g. SFTP).
3. Notify the supplier that a new delta file is available via **email** or a **FreshDesk support ticket**.

---

## 5. Expected Workflow for the Supplier

The supplier receives delta backup files and must merge them into their existing blob tables. Below is a recommended workflow.

### 5.1 Receive and Restore

1. **Receive** the backup file(s) via the agreed secure transfer process.
2. **Validate** file integrity (checksum/hash if provided).
3. **Restore** the full `.BAK` into a staging database. The schema (including FileTable definitions) is defined by the **originator of the data**; the supplier should restore into a compatible environment.

### 5.2 Merge into Target Tables

Apply “latest per blob” semantics: for each `stream_id`, keep the row with the latest `ModifiedOn`. Treat the delta as a set of candidate rows to merge.

**Merge logic (pseudocode):**

```
FOR each row R in the delta/staging table:
  IF R.stream_id EXISTS in target table:
    IF R.ModifiedOn > target.ModifiedOn:
      UPDATE target SET ... FROM R
  ELSE:
    INSERT R into target
```

**Example T-SQL pattern (conceptual; table names depend on your config):**

```sql
-- Merge delta into target (e.g. ReferralAttachment – adjust table names as per your schema)
MERGE TargetDB.dbo.ReferralAttachment AS t
USING StagingDB.dbo.ReferralAttachment AS s
ON t.stream_id = s.stream_id
WHEN MATCHED AND s.ModifiedOn > t.ModifiedOn THEN
    UPDATE SET
        name = s.name,
        file_stream = s.file_stream,
        path_locator = s.path_locator,
        parent_path_locator = s.parent_path_locator,
        ModifiedOn = s.ModifiedOn
        -- ... other columns as needed
WHEN NOT MATCHED BY TARGET THEN
    INSERT (stream_id, name, file_stream, path_locator, parent_path_locator, ModifiedOn, ...)
    VALUES (s.stream_id, s.name, s.file_stream, s.path_locator, s.parent_path_locator, s.ModifiedOn, ...);
```

- Ensure parent rows exist before children (hierarchy: roots → missing parents → children). The delta engine uses this order; the supplier’s merge should respect parent-child relationships or process in the same order.
- The natural key for deduplication is `stream_id`; `ModifiedOn` is used to decide which version to keep.

### 5.3 Post-Merge Steps

1. **Verify** row counts and spot-check a sample of merged records.
2. **Log** the merge (e.g. rows inserted, updated, skipped) for audit.
3. **Archive or delete** the staging data and backup file according to retention policy.

### 5.4 Deletions

Deletions are not managed in the current design. The delta pipeline does not propagate deletes to the supplier.

---

## 6. Retention and Missed Delta Window

### 6.1 Retention Policy

Delta backup files are retained for **1 week**. If the supplier misses this window (e.g. did not request or process a delta in time), a new delta must be requested via the **FreshDesk** support system before the retention period expires.

### 6.2 If the Retention Window Is Missed

If the supplier submits a request after the 1-week retention window, a manual refresh is required. This involves adjusting the high-watermark so the next delta run will re-include the missed period.

**Steps to refresh the delta (originator/admin):**

1. **Raise and process a FreshDesk ticket** requesting a delta refresh for the missed period.
2. **Identify the affected table(s)** and the date range to be re-included (e.g. the week that was missed).
3. **Update the high-watermark** in `BlobDeltaHighWatermark` to a value *before* the start of the missed window. This causes the next run to treat that period as unprocessed.

```sql
USE BlobDeltaJobs;
GO

-- Example: set LastHighWaterModifiedOn to 1 Jan 2025 so the next run
-- will pick up records from that date forward (within the window logic).
-- Replace the TableName and date with the actual table and desired start date.
UPDATE dbo.BlobDeltaHighWatermark
SET LastHighWaterModifiedOn = '2025-01-01 00:00:00'
WHERE TableName = N'Gwent_LA_FileTable.dbo.ReferralAttachment';  -- example; use your actual TableName
```

4. **Run the delta job** for the affected table(s):

```sql
EXEC dbo.usp_BlobDelta_RunOperator
    @Mode          = N'SingleTable',
    @TableName     = N'Gwent_LA_FileTable.dbo.ReferralAttachment',  -- or AllTables if multiple
    @BatchSize     = 500,
    @MaxDOP        = 2,
    @BusinessUnitId = NULL;
```

5. **Create a fresh backup** and transfer it to the supplier, then notify them via email or FreshDesk.

**Note:** Setting `LastHighWaterModifiedOn` to an earlier date means the next run will reprocess records from that point forward. The window logic applies: `WindowStart` will be derived from the updated watermark (minus the safety buffer), and `WindowEnd` will be the run start time minus the safety buffer. This effectively re-captures the missed period.

---

## 7. Quick Reference

| Object | Purpose |
|--------|---------|
| `BlobDeltaJobs` | Orchestration database for delta runs |
| `BlobDeltaTableConfig` | Per-table configuration (source, target, metadata, safety buffer) |
| `BlobDeltaHighWatermark` | Last processed `ModifiedOn` per table |
| `BlobDeltaRun` | Run headers (RunId, status, timestamps) |
| `BlobDeltaRunStep` | Per-step/batch progress |
| `usp_BlobDelta_RunOperator` | Main entry point for running delta loads |
