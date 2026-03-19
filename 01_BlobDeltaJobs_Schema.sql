-- =============================================================================
-- Blob Delta Jobs: Schema and Core Tables
-- -----------------------------------------------------------------------------
-- Purpose
--   New job-run database to support recurring blob delta loads, built as a
--   clean V3-style project that uses the V2 migration ideas (scripts in tables,
--   stages/steps, queues) but separates orchestration from the FileTable DB.
--
--   This script creates:
--     - BlobDeltaJobs database
--     - Core config / high-watermark / run / queue / script / deletion tables
--
--   The actual delta engine procedure(s) and script seed data will live in
--   separate scripts, e.g.:
--     - 04_BlobDeltaJobs_Seed_Config.sql
--     - 05_BlobDeltaJobs_Engine.sql
--
-- Usage
--   Run once on the SQL Server instance that hosts the FileTable and metadata
--   databases. The job DB is intended to live on the same instance so three-
--   part names can be used to reach source/target/metadata tables.
-- =============================================================================

IF DB_ID(N'BlobDeltaJobs') IS NULL
BEGIN
    PRINT N'Creating BlobDeltaJobs database...';
    CREATE DATABASE BlobDeltaJobs;
END;
GO

USE BlobDeltaJobs;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.TableConfig', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.TableConfig...';
    CREATE TABLE dbo.TableConfig
    (
        TableName              sysname        NOT NULL PRIMARY KEY,
            -- Logical key, typically the 3-part name of the target FileTable
            -- (e.g. Gwent_LA_FileTable.dbo.ReferralAttachment).

        SourceDatabase         sysname        NOT NULL,
        SourceSchema           sysname        NOT NULL,
        SourceTable            sysname        NOT NULL,

        TargetDatabase         sysname        NOT NULL,
        TargetSchema           sysname        NOT NULL,
        TargetTable            sysname        NOT NULL,

        MetadataDatabase       sysname        NOT NULL,
        MetadataSchema         sysname        NOT NULL,
        MetadataTable          sysname        NOT NULL,
        MetadataIdColumn       sysname        NOT NULL,
            -- Column in metadata table that links to stream_id in the blob table.

        MetadataModifiedOnCol  sysname        NOT NULL,
            -- Name of the [ModifiedOn] (or equivalent) column in the metadata
            -- table, used for delta-window filtering.

        SafetyBufferMinutes    int            NOT NULL
            CONSTRAINT DF_TableConfig_SafetyBufferMinutes DEFAULT (240),
            -- Safety buffer applied around the high-watermark to avoid missing
            -- late/overlapping updates. 240 = 4 hours by default.

        IncludeUpdatesInDelta  bit            NOT NULL
            CONSTRAINT DF_TableConfig_IncUpd DEFAULT (1),

        IncludeDeletesInDelta  bit            NOT NULL
            CONSTRAINT DF_TableConfig_IncDel DEFAULT (0),

        IsActive               bit            NOT NULL
            CONSTRAINT DF_TableConfig_IsActive DEFAULT (1),

        CreatedAt              datetime2(7)   NOT NULL
            CONSTRAINT DF_TableConfig_CreatedAt DEFAULT (SYSDATETIME()),
        UpdatedAt              datetime2(7)   NULL
    );
END;
GO

-- -----------------------------------------------------------------------------
-- 2. Per-table high-watermark and run lease
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.HighWatermark', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.HighWatermark...';
    CREATE TABLE dbo.HighWatermark
    (
        TableName                sysname       NOT NULL PRIMARY KEY
            REFERENCES dbo.TableConfig (TableName),

        LastHighWaterModifiedOn  datetime2(7)  NULL,
            -- Last metadata.ModifiedOn value that has been fully captured in
            -- all successful delta runs for this table.

        LastRunId                uniqueidentifier NULL,
        LastRunCompletedAt       datetime2(7)  NULL,

        IsInitialFullLoadDone    bit           NOT NULL
            CONSTRAINT DF_HighWatermark_Initial DEFAULT (0),

        IsRunning                bit           NOT NULL
            CONSTRAINT DF_HighWatermark_IsRunning DEFAULT (0),
            -- Logical lock indicator to prevent overlapping runs for the same
            -- table. The engine should also use RunLeaseExpiresAt as a timeout.

        RunLeaseExpiresAt        datetime2(7)  NULL
    );
END;
GO

-- -----------------------------------------------------------------------------
-- 3. Run header and per-step progress
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.Run', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.Run...';
    CREATE TABLE dbo.Run
    (
        RunId              uniqueidentifier NOT NULL PRIMARY KEY,

        RunType            nvarchar(20)     NOT NULL,
            -- e.g. 'Full' or 'Delta'.

        RequestedBy        sysname          NULL,

        RunStartedAt       datetime2(7)     NOT NULL,
        RunCompletedAt     datetime2(7)     NULL,

        Status             nvarchar(20)     NOT NULL,
            -- e.g. 'InProgress','Succeeded','Failed'.

        ErrorMessage       nvarchar(max)    NULL
    );
END;
GO

IF OBJECT_ID(N'dbo.RunStep', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.RunStep...';
    CREATE TABLE dbo.RunStep
    (
        RunId              uniqueidentifier NOT NULL
            REFERENCES dbo.Run (RunId),

        TableName          sysname          NOT NULL,

        StepNumber         tinyint          NOT NULL,
            -- 1 = Roots, 2 = MissingParents, 3 = Children (for initial design).

        BatchNumber        int              NOT NULL,
            -- 0 = synthetic summary row for the step; >0 = per-batch entry.

        RowsProcessed      int              NOT NULL,
        TotalRowsProcessed int              NOT NULL,

        WindowStart        datetime2(7)     NULL,
        WindowEnd          datetime2(7)     NULL,

        BatchStartedAt     datetime2(7)     NOT NULL,
        BatchCompletedAt   datetime2(7)     NOT NULL,

        Status             nvarchar(20)     NOT NULL,
            -- e.g. 'InProgress','Completed','Failed'.

        ErrorMessage       nvarchar(max)    NULL,

        CONSTRAINT PK_RunStep
            PRIMARY KEY (RunId, TableName, StepNumber, BatchNumber)
    );
END;
GO

-- -----------------------------------------------------------------------------
-- 4. Missing-parents queue for delta runs
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.MissingParentsQueue', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.MissingParentsQueue...';
    CREATE TABLE dbo.MissingParentsQueue
    (
        RunId        uniqueidentifier NOT NULL,
        TableName    sysname          NOT NULL,
        stream_id    uniqueidentifier NOT NULL,

        BusinessUnit uniqueidentifier NULL,
            -- Optional BU tag if needed for debugging/troubleshooting.

        Processed    bit              NOT NULL
            CONSTRAINT DF_MissingParentsQueue_Processed DEFAULT (0),

        CreatedAt    datetime2(7)     NOT NULL
            CONSTRAINT DF_MissingParentsQueue_CreatedAt DEFAULT (SYSDATETIME()),

        CONSTRAINT PK_MissingParentsQueue
            PRIMARY KEY (RunId, TableName, stream_id)
    );
END;
GO

-- -----------------------------------------------------------------------------
-- 5. Step script templates and queue population scripts
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.StepScript', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.StepScript...';
    CREATE TABLE dbo.StepScript
    (
        StepNumber          tinyint        NOT NULL,
        ScriptKind          nvarchar(50)   NOT NULL,
            -- e.g. 'Roots','MissingParentsBatch','Children'.

        StageName           nvarchar(50)   NOT NULL,
            -- Documentation-only label, e.g. 'Roots','MissingParents','Children'.

        ScriptBody          nvarchar(max)  NOT NULL,
            -- Template body with placeholders for tables/columns, plus
            -- runtime parameters such as @BatchSize, @WindowStart, @WindowEnd.

        UseParameterizedMaxDOP bit         NOT NULL,

        Description         nvarchar(256)  NULL,

        CONSTRAINT PK_StepScript
            PRIMARY KEY (StepNumber, ScriptKind)
    );
END;
GO

IF OBJECT_ID(N'dbo.QueuePopulationScript', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.QueuePopulationScript...';
    CREATE TABLE dbo.QueuePopulationScript
    (
        TableName  sysname        NOT NULL PRIMARY KEY,
            -- One row per logical table; script body populated by seed script.

        ScriptBody nvarchar(max)  NOT NULL
    );
END;
GO

-- -----------------------------------------------------------------------------
-- 6. Optional deletion log (for rare deletions)
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.DeletionLog', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.DeletionLog...';
    CREATE TABLE dbo.DeletionLog
    (
        TableName   sysname          NOT NULL,
        BlobId      uniqueidentifier NOT NULL,
            -- Typically the stream_id of the deleted blob.

        DeletedOn   datetime2(7)     NOT NULL,

        Source      nvarchar(128)    NULL,
        Reason      nvarchar(256)    NULL,

        CreatedAt   datetime2(7)     NOT NULL
            CONSTRAINT DF_BlobDeltaDelLog_CreatedAt DEFAULT (SYSDATETIME()),

        CONSTRAINT PK_DeletionLog
            PRIMARY KEY (TableName, BlobId, DeletedOn)
    );
END;
GO

-- -----------------------------------------------------------------------------
-- 7. Error logging (per-run, per-table/step/batch)
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.ErrorLog', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.ErrorLog...';
    CREATE TABLE dbo.ErrorLog
    (
        ErrorId         int             NOT NULL IDENTITY(1,1)
            CONSTRAINT PK_ErrorLog PRIMARY KEY,

        RunId           uniqueidentifier NOT NULL
            REFERENCES dbo.Run (RunId),

        TableName       sysname         NULL,
        StepNumber      tinyint         NULL,
        BatchNumber     int             NULL,

        ErrorScope      nvarchar(20)    NOT NULL,
            -- e.g. 'Table','Batch','Row'.

        ErrorNumber     int             NULL,
        ErrorSeverity   int             NULL,
        ErrorState      int             NULL,
        ErrorLine       int             NULL,
        ErrorProcedure  sysname         NULL,

        ErrorMessage    nvarchar(max)   NOT NULL,

        SourceKey       nvarchar(256)   NULL,
            -- Optional: business key or stream_id text for row-level context.

        OccurredAt      datetime2(7)    NOT NULL
            CONSTRAINT DF_ErrorLog_OccurredAt DEFAULT (SYSDATETIME())
    );

    CREATE NONCLUSTERED INDEX IX_ErrorLog_Run_Table_Step_Batch
        ON dbo.ErrorLog (RunId, TableName, StepNumber, BatchNumber, OccurredAt);
END;
GO

-- -----------------------------------------------------------------------------
-- 8. Error handling policy (fatal vs non-fatal classification)
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.ErrorPolicy', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.ErrorPolicy...';
    CREATE TABLE dbo.ErrorPolicy
    (
        ErrorPolicyId       int             NOT NULL IDENTITY(1,1)
            CONSTRAINT PK_ErrorPolicy PRIMARY KEY,

        ErrorNumber         int             NULL,
            -- When NULL, classification may be driven by ErrorMessagePattern only.

        ErrorMessagePattern nvarchar(4000)  NULL,
            -- Used with LIKE for pattern-based matching; NULL = match by ErrorNumber only.

        AppliesToStep       tinyint         NULL,
            -- NULL = all steps; otherwise restrict to a specific StepNumber.

        AppliesToTableName  sysname         NULL,
            -- NULL = all tables; otherwise restrict classification to specific logical table.

        IsFatal             bit             NOT NULL,
            -- 1 = treat matching errors as fatal; 0 = non-fatal (log and continue).

        ErrorScope          nvarchar(20)    NOT NULL,
            -- e.g. 'Table','Batch','Row' – indicates intended scope for handling.

        Notes               nvarchar(512)   NULL
    );

    CREATE NONCLUSTERED INDEX IX_ErrorPolicy_Lookup
        ON dbo.ErrorPolicy (ErrorNumber, AppliesToTableName, AppliesToStep);
END;
GO

-- -----------------------------------------------------------------------------
-- 9. Target database filter list for multi-database runs
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.TargetDatabases', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.TargetDatabases...';
    CREATE TABLE dbo.TargetDatabases
    (
        TargetDatabase sysname NOT NULL PRIMARY KEY,
            -- Logical database name matching TableConfig.TargetDatabase.

        Extract       bit     NOT NULL
            CONSTRAINT DF_TargetDatabases_Extract DEFAULT (1)
            -- When 1 and @TargetDatabase IS NULL, tables for this TargetDatabase
            -- are eligible for processing; when 0, they are skipped unless the
            -- caller explicitly specifies @TargetDatabase.
    );
END;
GO

PRINT N'BlobDeltaJobs core schema created/verified successfully.';
GO
