-- Destructively clear all data from FILETABLEs in the target database
-- without dropping the tables or FILESTREAM filegroup.
-- WARNING: This script deletes all FILETABLE rows (except the implicit root
--          folder entry). All FILETABLE data will be permanently deleted.
-------------------------------------------------------------------------------
DECLARE @TargetDatabase sysname       = N'Primer_FileTable'; -- <== change to your DB, e.g. N'YnysMon_FileTable'
DECLARE @Sql            nvarchar(MAX);

-- 1) Delete all rows from FILETABLEs in the target database
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
DECLARE @deleteSql nvarchar(MAX) = N''''; 

SELECT @deleteSql = @deleteSql +
       ''DELETE FROM '' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + ''.'' + QUOTENAME(t.name) +
       '' WHERE parent_path_locator IS NOT NULL;'' + CHAR(13) + CHAR(10)
FROM sys.tables t
WHERE t.is_filetable = 1;

IF @deleteSql <> N''''
BEGIN
    PRINT ''Deleting all rows from FILETABLEs in database ' + @TargetDatabase + N'...'';
    EXEC (@deleteSql);
END;
';

EXEC (@Sql);

-- 2) Force FILESTREAM garbage collection so underlying files are cleaned up
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF OBJECT_ID(N''sys.sp_filestream_force_garbage_collection'') IS NOT NULL
BEGIN
    PRINT ''Forcing FILESTREAM garbage collection in database ' + @TargetDatabase + N'...'';
    EXEC sys.sp_filestream_force_garbage_collection @dbname = N''' + @TargetDatabase + N''';
    EXEC sys.sp_filestream_force_garbage_collection @dbname = N''' + @TargetDatabase + N''';
    CHECKPOINT;
END
ELSE
BEGIN
    PRINT ''WARNING: sys.sp_filestream_force_garbage_collection is not available on this server. '';
    PRINT ''         FILESTREAM files may remain on disk until normal garbage collection runs.'';
END;
';
EXEC (@Sql);

PRINT 'Truncation-style cleanup of FILETABLE data completed for database ' + QUOTENAME(@TargetDatabase) + '.';