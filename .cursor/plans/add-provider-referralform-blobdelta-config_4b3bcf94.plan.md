---
name: add-provider-referralform-blobdelta-config
overview: Extend the BlobDeltaJobs seed configuration to include ProviderAttachment and ReferralFormAttachment filetables using the existing ClientAttachment and ReferralAttachment pattern.
todos:
  - id: add-provider-referralform-config-vars
    content: Add ProviderAttachment and ReferralFormAttachment table and metadata variables to 04_BlobDeltaJobs_Seed_Config.sql configuration defaults section.
    status: completed
  - id: extend-blobdeltatableconfig-merge
    content: Extend BlobDeltaTableConfig MERGE USING dataset with ProviderAttachment and ReferralFormAttachment rows mirroring existing attachment config.
    status: completed
  - id: update-queue-population-source-filter
    content: Update BlobDeltaQueuePopulationScript MERGE source filter to include ProviderAttachment and ReferralFormAttachment source tables.
    status: completed
isProject: false
---

## Goal

Add `ProviderAttachment` and `ReferralFormAttachment` into the Blob Delta extraction/configuration pipeline in `BlobDeltaJobs` so they follow the same pattern as the existing `ReferralAttachment` and `ClientAttachment` tables, including metadata linkage and queue population.

## Key Files

- `[04_BlobDeltaJobs_Seed_Config.sql](./04_BlobDeltaJobs_Seed_Config.sql)`

## Plan

- **Introduce new configuration variables**
  - In the "Configuration defaults" section, add new `sysname` variables for the two additional filetables:
    - `@ProviderTableName = N'ProviderAttachment'`
    - `@ReferralFormTableName = N'ReferralFormAttachment'`
  - Add corresponding metadata table and ID-column variables, mirroring the existing naming pattern:
    - `@ProviderMetadataTable = N'cw_ProviderAttachmentBase'`
    - `@ProviderMetadataIdColumn = N'cw_ProviderAttachmentId'` (adjust if actual PK name differs)
    - `@ReferralFormMetadataTable = N'cw_ReferralFormAttachmentBase'`
    - `@ReferralFormMetadataIdColumn = N'cw_ReferralFormAttachmentId'` (adjust if actual PK name differs)
- **Extend `BlobDeltaTableConfig` seeding for new tables**
  - In the `MERGE dbo.BlobDeltaTableConfig AS t` statement, inside the `USING` CTE that currently has two `SELECT` branches (for `@ReferralTableName` and `@ClientTableName`), add two more `UNION ALL` branches, structurally identical, for the new tables:

```sql
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @ProviderTableName,
        @SourceDatabase,  @SourceSchema,  @ProviderTableName,
        @FileTableDatabase, @FileTableSchema, @ProviderTableName,
        @MetadataDatabase, @MetadataSchema, @ProviderMetadataTable,
        @ProviderMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @ReferralFormTableName,
        @SourceDatabase,  @SourceSchema,  @ReferralFormTableName,
        @FileTableDatabase, @FileTableSchema, @ReferralFormTableName,
        @MetadataDatabase, @MetadataSchema, @ReferralFormMetadataTable,
        @ReferralFormMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
```

- Leave the `WHEN MATCHED` update clause unchanged; it will work for the new rows as well.
- Rely on the existing `MERGE dbo.BlobDeltaHighWatermark` to automatically create high-watermark rows for the two new tables (no change needed there).
- **Update queue population script targeting filter**
  - In the `MERGE dbo.BlobDeltaQueuePopulationScript AS t` section at the bottom of the script, widen the `SourceTable` filter so queue scripts are generated for the new tables as well.
  - Change:

```sql
    WHERE SourceTable IN (N'ReferralAttachment', N'ClientAttachment')
```

- To include the two new tables:

```sql
    WHERE SourceTable IN (
        N'ReferralAttachment',
        N'ClientAttachment',
        N'ProviderAttachment',
        N'ReferralFormAttachment'
    )
```

- **Execution and verification**
  - Re-run `04_BlobDeltaJobs_Seed_Config.sql` against the `BlobDeltaJobs` database.
  - Verify rows now exist in `dbo.BlobDeltaTableConfig` and `dbo.BlobDeltaHighWatermark` for the four filetables, and that `dbo.BlobDeltaQueuePopulationScript` contains one row per `TableName` with the shared script body.

## Notes / Assumptions

- Assumes the metadata table and primary key column names follow the same naming pattern as existing attachments; if the actual column names differ, only the `@ProviderMetadataIdColumn` and `@ReferralFormMetadataIdColumn` assignments need to be adjusted.
- The step script templates are generic over `[SourceTableFull]` and `[MetadataTableFull]`, so no changes are needed there for these new tables.

