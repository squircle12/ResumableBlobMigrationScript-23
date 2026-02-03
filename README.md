# Blob Migration V2

Version 2 of the blob migration approach: **script-in-table** with a single master procedure that runs migration for any configured table (ReferralAttachment, ClientAttachment, and future tables).

## Contents

| File | Purpose |
|------|--------|
| **README_V2_Approach.md** | Approach review, variable handling, and **decisions** (answers to clarifying questions). |
| **00_Schema_V2.sql** | Schema: progress (PK RunId, TableName, Step, BatchNumber), queue, **BlobMigrationTableConfig**, **BlobMigrationStepScript** (with StageName), **BlobMigrationQueuePopulationScript**. Idempotent. |
| **01_Seed_DefaultScripts.sql** | Seed: table config (ReferralAttachment, ClientAttachment), step scripts (StageName Roots/MissingParents/Children), queue population scripts per table. Idempotent MERGE. |
| **02_usp_BlobMigration_Run_V2.sql** | Master procedure (stub). To be implemented using script table and config. |

## Deployment order

1. Run **00_Schema_V2.sql** in `Gwent_LA_FileTable`.
2. Run **01_Seed_DefaultScripts.sql** to load default table config and step scripts.
3. Implement and deploy **02_usp_BlobMigration_Run_V2.sql** (design decisions recorded in README_V2_Approach.md).

## Placeholders in scripts

Step scripts in `BlobMigrationStepScript` use these placeholders; the proc replaces them at runtime from `BlobMigrationTableConfig` and parameters:

- `[SourceTableFull]` = SourceDatabase.SourceSchema.SourceTable  
- `[TargetTableFull]` = TargetDatabase.TargetSchema.TargetTable  
- `[MetadataTableFull]` = MetadataDatabase.MetadataSchema.MetadataTable  
- `[MetadataIdColumn]` = MetadataIdColumn  
- `[MaxDOP]` = numeric value (Step 1 only; proc concatenates into script)

Parameters passed via `sp_executesql` (not replaced): `@BatchSize`, `@ExcludedStreamId`, and for Step 2 queue population `@RunId`, `@TableName`.
