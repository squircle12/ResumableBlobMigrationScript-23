-------------------------------------------------------------------------------
-- Destructively clear all data from FILETABLEs in the target database
-- without dropping the tables or FILESTREAM filegroup.
--
-- WARNING: This script deletes all FILETABLE rows (except the implicit root
--          folder entry). All FILETABLE data will be permanently deleted
--          and the underlying FILESTREAM files reclaimed from disk.
--
-- Steps:
--   1) DELETE all rows from every FILETABLE (preserves the root folder row)
--   2) Truncate the log (BACKUP LOG TO NUL) so GC can release FILESTREAM files
--   3) CHECKPOINT + force FILESTREAM garbage collection (twice)
-------------------------------------------------------------------------------

DECLARE @TargetDatabase sysname = N'Primer_FileTable'; -- <== change to your DB
DECLARE @Sql            nvarchar(MAX);

-------------------------------------------------------------------------------
-- 1) Delete all rows from FILETABLEs (keep the root row where name IS NULL)
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
DECLARE @deleteSql nvarchar(MAX) = N'''';
SELECT @deleteSql = @deleteSql +
       ''DELETE FROM '' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + ''.'' + QUOTENAME(t.name) +
       '' WHERE name IS NOT NULL;'' + CHAR(13) + CHAR(10)
FROM sys.tables t
WHERE t.is_filetable = 1;

IF @deleteSql <> N''''
BEGIN
    PRINT ''Deleting all rows from FILETABLEs in database ' + @TargetDatabase + N'...'';
    EXEC (@deleteSql);
END;
';
EXEC (@Sql);

-------------------------------------------------------------------------------
-- 2) Truncate the log so GC can release FILESTREAM files
-------------------------------------------------------------------------------
SET @Sql = N'
DECLARE @recovery sysname;
SELECT @recovery = recovery_model_desc FROM sys.databases WHERE name = ''' + @TargetDatabase + N''';

IF @recovery IN (''FULL'', ''BULK_LOGGED'')
BEGIN
    PRINT ''Backing up log for ' + @TargetDatabase + N' to NUL to allow GC...'';
    BACKUP LOG ' + QUOTENAME(@TargetDatabase) + N' TO DISK = N''NUL'';
END
ELSE
BEGIN
    PRINT ''Database ' + @TargetDatabase + N' is in SIMPLE recovery - checkpoint will release log.'';
END;
';
EXEC (@Sql);

-------------------------------------------------------------------------------
-- 3) Force FILESTREAM garbage collection
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
PRINT ''Forcing FILESTREAM garbage collection in database ' + @TargetDatabase + N'...'';
CHECKPOINT;
EXEC sys.sp_filestream_force_garbage_collection @dbname = N''' + @TargetDatabase + N''';
EXEC sys.sp_filestream_force_garbage_collection @dbname = N''' + @TargetDatabase + N''';
CHECKPOINT;
';
EXEC (@Sql);

PRINT 'Truncation-style cleanup of FILETABLE data completed for database ' + QUOTENAME(@TargetDatabase) + '.';