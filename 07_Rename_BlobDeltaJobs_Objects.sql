-- =============================================================================
-- Blob Delta Jobs: One-shot rename of BlobDelta* objects to shorter names
-- -----------------------------------------------------------------------------
-- Purpose
--   Run once against an existing BlobDeltaJobs database to rename tables,
--   views, and supporting objects that still use the BlobDelta* prefix to the
--   new shorter names used by the updated scripts.
--
--   This script is idempotent: each rename is guarded by existence checks and
--   will be skipped if the target name already exists.
--
-- Usage
--   1. Take a backup of the BlobDeltaJobs database.
--   2. Connect to the BlobDeltaJobs database.
--   3. Execute this script once during a maintenance window.
-- =============================================================================

SET NOCOUNT ON;

PRINT N'Starting BlobDeltaJobs object rename...';

-------------------------------------------------------------------------------
-- Helper: safe table rename (old name -> new name)
-------------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.BlobDeltaTableConfig', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.TableConfig', N'U') IS NULL
BEGIN
    PRINT N'Renaming dbo.BlobDeltaTableConfig -> dbo.TableConfig...';
    EXEC sp_rename N'dbo.BlobDeltaTableConfig', N'TableConfig', N'OBJECT';
END

IF OBJECT_ID(N'dbo.BlobDeltaHighWatermark', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.HighWatermark', N'U') IS NULL
BEGIN
    PRINT N'Renaming dbo.BlobDeltaHighWatermark -> dbo.HighWatermark...';
    EXEC sp_rename N'dbo.BlobDeltaHighWatermark', N'HighWatermark', N'OBJECT';
END

IF OBJECT_ID(N'dbo.BlobDeltaRun', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.Run', N'U') IS NULL
BEGIN
    PRINT N'Renaming dbo.BlobDeltaRun -> dbo.Run...';
    EXEC sp_rename N'dbo.BlobDeltaRun', N'Run', N'OBJECT';
END

IF OBJECT_ID(N'dbo.BlobDeltaRunStep', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.RunStep', N'U') IS NULL
BEGIN
    PRINT N'Renaming dbo.BlobDeltaRunStep -> dbo.RunStep...';
    EXEC sp_rename N'dbo.BlobDeltaRunStep', N'RunStep', N'OBJECT';
END

IF OBJECT_ID(N'dbo.BlobDeltaMissingParentsQueue', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.MissingParentsQueue', N'U') IS NULL
BEGIN
    PRINT N'Renaming dbo.BlobDeltaMissingParentsQueue -> dbo.MissingParentsQueue...';
    EXEC sp_rename N'dbo.BlobDeltaMissingParentsQueue', N'MissingParentsQueue', N'OBJECT';
END

IF OBJECT_ID(N'dbo.BlobDeltaStepScript', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.StepScript', N'U') IS NULL
BEGIN
    PRINT N'Renaming dbo.BlobDeltaStepScript -> dbo.StepScript...';
    EXEC sp_rename N'dbo.BlobDeltaStepScript', N'StepScript', N'OBJECT';
END

IF OBJECT_ID(N'dbo.BlobDeltaQueuePopulationScript', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.QueuePopulationScript', N'U') IS NULL
BEGIN
    PRINT N'Renaming dbo.BlobDeltaQueuePopulationScript -> dbo.QueuePopulationScript...';
    EXEC sp_rename N'dbo.BlobDeltaQueuePopulationScript', N'QueuePopulationScript', N'OBJECT';
END

IF OBJECT_ID(N'dbo.BlobDeltaDeletionLog', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.DeletionLog', N'U') IS NULL
BEGIN
    PRINT N'Renaming dbo.BlobDeltaDeletionLog -> dbo.DeletionLog...';
    EXEC sp_rename N'dbo.BlobDeltaDeletionLog', N'DeletionLog', N'OBJECT';
END

IF OBJECT_ID(N'dbo.BlobDeltaErrorLog', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.ErrorLog', N'U') IS NULL
BEGIN
    PRINT N'Renaming dbo.BlobDeltaErrorLog -> dbo.ErrorLog...';
    EXEC sp_rename N'dbo.BlobDeltaErrorLog', N'ErrorLog', N'OBJECT';
END

IF OBJECT_ID(N'dbo.BlobDeltaErrorPolicy', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.ErrorPolicy', N'U') IS NULL
BEGIN
    PRINT N'Renaming dbo.BlobDeltaErrorPolicy -> dbo.ErrorPolicy...';
    EXEC sp_rename N'dbo.BlobDeltaErrorPolicy', N'ErrorPolicy', N'OBJECT';
END

IF OBJECT_ID(N'dbo.BlobDeltaTargetDatabases', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.TargetDatabases', N'U') IS NULL
BEGIN
    PRINT N'Renaming dbo.BlobDeltaTargetDatabases -> dbo.TargetDatabases...';
    EXEC sp_rename N'dbo.BlobDeltaTargetDatabases', N'TargetDatabases', N'OBJECT';
END

-------------------------------------------------------------------------------
-- Views
-------------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.vw_BlobDeltaRunSummary', N'V') IS NOT NULL
   AND OBJECT_ID(N'dbo.vw_RunSummary', N'V') IS NULL
BEGIN
    PRINT N'Renaming dbo.vw_BlobDeltaRunSummary -> dbo.vw_RunSummary...';
    EXEC sp_rename N'dbo.vw_BlobDeltaRunSummary', N'vw_RunSummary', N'OBJECT';
END

PRINT N'BlobDeltaJobs object rename completed.';

