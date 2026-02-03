-- =============================================================================
-- Blob Migration V2: Schema (state, queue, table config, step scripts)
-- Database: Gwent_LA_FileTable
-- Idempotent: creates tables/indexes only if they do not exist.
-- Run 01_Seed_DefaultScripts.sql after this to load default table config and scripts.
-- =============================================================================

USE Gwent_LA_FileTable;
GO

-- -----------------------------------------------------------------------------
-- 1. Progress table: one row per batch
-- PK (RunId, TableName, Step, BatchNumber) so one RunId can track multiple tables.
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dbo.BlobMigrationProgress', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.BlobMigrationProgress
    (
        RunId              UNIQUEIDENTIFIER NOT NULL,
        TableName          NVARCHAR(259)    NOT NULL,
        RunStartedAt       DATETIME2(7)     NOT NULL,
        Step               TINYINT          NOT NULL,
        BatchNumber        INT              NOT NULL,
        RowsInserted       INT              NOT NULL,
        TotalRowsInserted  INT              NOT NULL,
        BatchStartedAt     DATETIME2(7)     NOT NULL,
        BatchCompletedAt   DATETIME2(7)     NOT NULL,
        Status             VARCHAR(20)      NOT NULL,
        ErrorMessage       NVARCHAR(MAX)    NULL,
        CONSTRAINT PK_BlobMigrationProgress PRIMARY KEY (RunId, TableName, Step, BatchNumber)
    );

    CREATE NONCLUSTERED INDEX IX_BlobMigrationProgress_RunId_Step
        ON dbo.BlobMigrationProgress (RunId, TableName, Step);

    CREATE NONCLUSTERED INDEX IX_BlobMigrationProgress_TableName_RunId
        ON dbo.BlobMigrationProgress (TableName, RunId);

    PRINT 'Created dbo.BlobMigrationProgress.';
END
ELSE
    PRINT 'dbo.BlobMigrationProgress already exists; skipping.';
GO

-- Add TableName to Progress if table exists but column is missing (idempotent)
IF EXISTS (SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('dbo.BlobMigrationProgress'))
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.BlobMigrationProgress') AND name = 'TableName')
BEGIN
    ALTER TABLE dbo.BlobMigrationProgress
        ADD TableName NVARCHAR(259) NOT NULL DEFAULT N'Gwent_LA_FileTable.dbo.ReferralAttachment';
    CREATE NONCLUSTERED INDEX IX_BlobMigrationProgress_TableName_RunId
        ON dbo.BlobMigrationProgress (TableName, RunId);
    PRINT 'Added TableName to dbo.BlobMigrationProgress.';
END
GO

-- Migrate Progress PK from (RunId, Step, BatchNumber) to (RunId, TableName, Step, BatchNumber) if needed
IF EXISTS (SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('dbo.BlobMigrationProgress'))
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.BlobMigrationProgress') AND name = 'TableName')
   AND EXISTS (SELECT 1 FROM sys.key_constraints k
                INNER JOIN sys.index_columns ic ON ic.object_id = k.parent_object_id AND ic.index_id = k.unique_index_id
                INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                WHERE k.parent_object_id = OBJECT_ID('dbo.BlobMigrationProgress') AND k.name = 'PK_BlobMigrationProgress'
                  AND c.name = 'RunId')
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints k
                   INNER JOIN sys.index_columns ic ON ic.object_id = k.parent_object_id AND ic.index_id = k.unique_index_id
                   INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                   WHERE k.parent_object_id = OBJECT_ID('dbo.BlobMigrationProgress') AND k.name = 'PK_BlobMigrationProgress'
                     AND c.name = 'TableName')
BEGIN
    ALTER TABLE dbo.BlobMigrationProgress DROP CONSTRAINT PK_BlobMigrationProgress;
    ALTER TABLE dbo.BlobMigrationProgress ADD CONSTRAINT PK_BlobMigrationProgress
        PRIMARY KEY (RunId, TableName, Step, BatchNumber);
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.BlobMigrationProgress') AND name = 'IX_BlobMigrationProgress_RunId_Step')
        DROP INDEX IX_BlobMigrationProgress_RunId_Step ON dbo.BlobMigrationProgress;
    CREATE NONCLUSTERED INDEX IX_BlobMigrationProgress_RunId_Step
        ON dbo.BlobMigrationProgress (RunId, TableName, Step);
    PRINT 'Migrated BlobMigrationProgress PK to (RunId, TableName, Step, BatchNumber).';
END
GO

-- -----------------------------------------------------------------------------
-- 2. Step 2 queue: missing parents to process (same as V1)
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dbo.BlobMigration_MissingParentsQueue', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.BlobMigration_MissingParentsQueue
    (
        RunId      UNIQUEIDENTIFIER NOT NULL,
        TableName  NVARCHAR(259)    NOT NULL,
        stream_id  UNIQUEIDENTIFIER NOT NULL,
        Processed  BIT              NOT NULL DEFAULT 0,
        CreatedAt  DATETIME2(7)     NOT NULL DEFAULT SYSDATETIME(),
        CONSTRAINT PK_BlobMigration_MissingParentsQueue PRIMARY KEY (RunId, stream_id)
    );

    CREATE NONCLUSTERED INDEX IX_BlobMigration_MissingParentsQueue_RunId_Processed
        ON dbo.BlobMigration_MissingParentsQueue (RunId, Processed)
        INCLUDE (stream_id);

    CREATE NONCLUSTERED INDEX IX_BlobMigration_MissingParentsQueue_TableName_RunId
        ON dbo.BlobMigration_MissingParentsQueue (TableName, RunId);

    PRINT 'Created dbo.BlobMigration_MissingParentsQueue.';
END
ELSE
    PRINT 'dbo.BlobMigration_MissingParentsQueue already exists; skipping.';
GO

-- Add TableName to Queue if table exists but column is missing (idempotent)
IF EXISTS (SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('dbo.BlobMigration_MissingParentsQueue'))
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.BlobMigration_MissingParentsQueue') AND name = 'TableName')
BEGIN
    ALTER TABLE dbo.BlobMigration_MissingParentsQueue
        ADD TableName NVARCHAR(259) NOT NULL DEFAULT N'Gwent_LA_FileTable.dbo.ReferralAttachment';
    CREATE NONCLUSTERED INDEX IX_BlobMigration_MissingParentsQueue_TableName_RunId
        ON dbo.BlobMigration_MissingParentsQueue (TableName, RunId);
    PRINT 'Added TableName to dbo.BlobMigration_MissingParentsQueue.';
END
GO

-- -----------------------------------------------------------------------------
-- 3. Table config: one row per migrated table (source/target/metadata)
-- Placeholders in step scripts: [SourceTableFull], [TargetTableFull],
-- [MetadataTableFull], [MetadataIdColumn]. ExcludedStreamId: global for all tables (config column unused).
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dbo.BlobMigrationTableConfig', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.BlobMigrationTableConfig
    (
        TableName          NVARCHAR(259)    NOT NULL,
        SourceDatabase     NVARCHAR(128)    NOT NULL,
        SourceSchema       NVARCHAR(128)    NOT NULL,
        SourceTable        NVARCHAR(128)   NOT NULL,
        TargetDatabase     NVARCHAR(128)    NOT NULL,
        TargetSchema       NVARCHAR(128)    NOT NULL,
        TargetTable        NVARCHAR(128)    NOT NULL,
        MetadataDatabase   NVARCHAR(128)    NOT NULL,
        MetadataSchema     NVARCHAR(128)    NOT NULL,
        MetadataTable      NVARCHAR(128)    NOT NULL,
        MetadataIdColumn   NVARCHAR(128)    NOT NULL,
        ExcludedStreamId   UNIQUEIDENTIFIER NULL,
        IsActive           BIT              NOT NULL DEFAULT 1,
        CreatedAt          DATETIME2(7)     NOT NULL DEFAULT SYSDATETIME(),
        UpdatedAt         DATETIME2(7)     NULL,
        CONSTRAINT PK_BlobMigrationTableConfig PRIMARY KEY (TableName)
    );
    PRINT 'Created dbo.BlobMigrationTableConfig.';
END
ELSE
    PRINT 'dbo.BlobMigrationTableConfig already exists; skipping.';
GO

-- -----------------------------------------------------------------------------
-- 4. Step scripts: templates per step (and ScriptKind for Step 2 batch only)
-- Placeholders replaced at runtime from BlobMigrationTableConfig + @MaxDOP.
-- StageName = documentation only (e.g. Roots, MissingParents, Children).
-- Step 2 queue population: table-specific scripts in BlobMigrationQueuePopulationScript.
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dbo.BlobMigrationStepScript', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.BlobMigrationStepScript
    (
        StepNumber         TINYINT          NOT NULL,
        ScriptKind         VARCHAR(30)     NOT NULL,
        StageName          NVARCHAR(100)   NULL,
        ScriptBody         NVARCHAR(MAX)   NOT NULL,
        UseParameterizedMaxDOP BIT         NOT NULL DEFAULT 0,
        Description        NVARCHAR(500)   NULL,
        CONSTRAINT PK_BlobMigrationStepScript PRIMARY KEY (StepNumber, ScriptKind)
    );
    PRINT 'Created dbo.BlobMigrationStepScript.';
END
ELSE
    PRINT 'dbo.BlobMigrationStepScript already exists; skipping.';
GO

-- Add StageName to StepScript if missing (idempotent)
IF EXISTS (SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('dbo.BlobMigrationStepScript'))
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.BlobMigrationStepScript') AND name = 'StageName')
BEGIN
    ALTER TABLE dbo.BlobMigrationStepScript ADD StageName NVARCHAR(100) NULL;
    PRINT 'Added StageName to dbo.BlobMigrationStepScript.';
END
GO

-- -----------------------------------------------------------------------------
-- 5. Queue population scripts: one row per table (ReferralAttachment, ClientAttachment)
-- Scripts "remain as they are" per table; placeholders replaced from TableConfig.
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dbo.BlobMigrationQueuePopulationScript', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.BlobMigrationQueuePopulationScript
    (
        TableName          NVARCHAR(259)   NOT NULL,
        ScriptBody         NVARCHAR(MAX)   NOT NULL,
        CONSTRAINT PK_BlobMigrationQueuePopulationScript PRIMARY KEY (TableName)
    );
    PRINT 'Created dbo.BlobMigrationQueuePopulationScript.';
END
ELSE
    PRINT 'dbo.BlobMigrationQueuePopulationScript already exists; skipping.';
GO
