-- =============================================================================
-- Blob Delta Jobs: Set High-Watermark for Delta Extracts
-- -----------------------------------------------------------------------------
-- Purpose
--   Convenience script to set the per-table high-watermark used by the
--   BlobDeltaJobs delta engine (dbo.BlobDeltaHighWatermark.LastHighWaterModifiedOn).
--
--   This is intended for scenarios such as:
--     - You have restored the source/metadata database from a point-in-time
--       backup and want delta extracts to start from that restoration point.
--     - You want to advance or reset the high-watermark for one or more tables
--       without modifying the engine or seed scripts.
--
-- Usage
--   1. Set @NewHighWaterUtc to the desired high-water datetime (in UTC or in
--      the same time zone as your metadata ModifiedOn column).
--   2. Optionally set:
--        @TargetDatabaseFilter to limit to a specific FileTable DB
--        @TableNameFilter     to limit to a single logical table
--   3. Leave @ApplyUpdates = 0 to see what WOULD be updated.
--   4. When you are happy, set @ApplyUpdates = 1 and re-run the script.
--
--   Notes
--   - High-watermark rows live ONLY in dbo.BlobDeltaHighWatermark; you do not
--     need to change 03/04/05 scripts to adjust the delta window.
--   - The delta engine computes @WindowStart/@WindowEnd from this value and
--     the per-table SafetyBufferMinutes in dbo.BlobDeltaTableConfig.
-- =============================================================================

USE BlobDeltaJobs;
GO

SET NOCOUNT ON;

DECLARE @NewHighWaterUtc      datetime2(7) = '2025-01-01T00:00:00'; -- TODO: set to source DB restore datetime
DECLARE @TargetDatabaseFilter sysname      = NULL;                  -- e.g. N'Gwent_LA_FileTable' or NULL for all
DECLARE @TableNameFilter      sysname      = NULL;                  -- e.g. N'Gwent_LA_FileTable.dbo.ReferralAttachment'
DECLARE @ApplyUpdates         bit          = 0;                     -- Safety flag: 0 = dry run, 1 = perform UPDATE

PRINT N'Previewing BlobDeltaHighWatermark rows that match the filters...';
PRINT N'  @NewHighWaterUtc      = ' + CONVERT(nvarchar(30), @NewHighWaterUtc, 126);
PRINT N'  @TargetDatabaseFilter = ' + ISNULL(@TargetDatabaseFilter, N'<ALL>');
PRINT N'  @TableNameFilter      = ' + ISNULL(@TableNameFilter, N'<ALL>');
PRINT N'  @ApplyUpdates         = ' + CAST(@ApplyUpdates AS nvarchar(1));
PRINT N'';

;WITH Targets AS
(
    SELECT
        h.TableName,
        h.LastHighWaterModifiedOn,
        h.LastRunId,
        h.LastRunCompletedAt,
        h.IsInitialFullLoadDone,
        h.IsRunning,
        h.RunLeaseExpiresAt,
        c.TargetDatabase,
        c.SourceDatabase,
        c.MetadataDatabase
    FROM dbo.BlobDeltaHighWatermark h
    INNER JOIN dbo.BlobDeltaTableConfig c
        ON c.TableName = h.TableName
    WHERE (@TargetDatabaseFilter IS NULL OR c.TargetDatabase = @TargetDatabaseFilter)
      AND (@TableNameFilter IS NULL OR h.TableName = @TableNameFilter)
)
SELECT
    TableName,
    TargetDatabase,
    SourceDatabase,
    MetadataDatabase,
    LastHighWaterModifiedOn,
    LastRunId,
    LastRunCompletedAt,
    IsInitialFullLoadDone,
    IsRunning,
    RunLeaseExpiresAt
FROM Targets
ORDER BY TableName;

IF @ApplyUpdates = 0
BEGIN
    PRINT N'';
    PRINT N'@ApplyUpdates = 0 (dry run). No high-watermark rows have been changed.';
    PRINT N'If the above preview looks correct, set @ApplyUpdates = 1 and re-run this script.';
    RETURN;
END;

PRINT N'';
PRINT N'Applying high-watermark update...';

DECLARE @RowsAffected int = 0;

;WITH Targets AS
(
    SELECT
        h.TableName
    FROM dbo.BlobDeltaHighWatermark h
    INNER JOIN dbo.BlobDeltaTableConfig c
        ON c.TableName = h.TableName
    WHERE (@TargetDatabaseFilter IS NULL OR c.TargetDatabase = @TargetDatabaseFilter)
      AND (@TableNameFilter IS NULL OR h.TableName = @TableNameFilter)
)
UPDATE h
SET
    LastHighWaterModifiedOn = @NewHighWaterUtc,
    LastRunId               = NULL,
    LastRunCompletedAt      = NULL,
    IsInitialFullLoadDone   = 1,           -- Treat everything up to @NewHighWaterUtc as fully loaded
    IsRunning               = 0,
    RunLeaseExpiresAt       = NULL
FROM dbo.BlobDeltaHighWatermark h
INNER JOIN Targets t
    ON t.TableName = h.TableName;

SET @RowsAffected = @@ROWCOUNT;

PRINT N'High-watermark update complete. Rows affected: ' + CAST(@RowsAffected AS nvarchar(20));
PRINT N'';
PRINT N'You can re-run the preview section above to confirm the new LastHighWaterModifiedOn values.';

GO

