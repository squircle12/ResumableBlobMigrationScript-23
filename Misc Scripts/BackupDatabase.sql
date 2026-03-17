/*****************************************************************************************
 Multi-part (striped) FULL backup script for large SQL Server databases
 -----------------------------------------------------------------------------------------
 - Backs up a single database to multiple .bak files on LOCAL DISK
 - Uses backup stripes to improve throughput (recommended for 300GB+ databases)
 - Includes options for compression, checksum, and progress reporting
 - Includes RESTORE VERIFYONLY step and example RESTORE DATABASE commands

 USAGE (manual run):
 1. Set the configuration variables in the "CONFIGURATION" section:
      - @DbName          : Name of the database to back up
      - @BackupPath      : Local directory where backup files will be written
      - @StripeCount     : Number of backup files (stripes) to create
      - @UseCompression  : 1 to enable BACKUP COMPRESSION (if supported)
      - @UseChecksum     : 1 to enable CHECKSUM during backup
      - @Overwrite       : 1 to allow overwriting existing backup files (INIT)
      - @UseFormat       : 1 to FORMAT the media (use with caution; usually 0)
 2. Run the whole script.
 3. After completion, a RESTORE VERIFYONLY will run against the created files.
 4. Use the RESTORE DATABASE examples at the bottom as a template when restoring.

 NOTES:
 - Choose @StripeCount based on number of physical disks / throughput (e.g. 4–8).
 - Keep all stripe files together; every file is required for restore.
 - Typically set @UseFormat = 0 unless you explicitly want to reinitialize the media.
 - Typically set @Overwrite = 1 when writing to fresh backup files in a dedicated folder.

******************************************************************************************/

/********************************************
 CONFIGURATION
*********************************************/
DECLARE
      @DbName         sysname        = N'LANameHere_LA_FileTable'               -- database to back up
    , @BackupPath     nvarchar(260)  = N'D:\MSSQL\MSSQL15.MSSQLSERVER\MSSQL\'   -- must end with backslash
    , @StripeCount    int            = 4                                        -- number of .bak files
    , @UseCompression bit            = 1                                        -- 1 = use COMPRESSION
    , @UseChecksum    bit            = 1                                        -- 1 = use CHECKSUM
    , @Overwrite      bit            = 1                                        -- 1 = INIT (overwrite)
    , @UseFormat      bit            = 0                                        -- 1 = FORMAT media (use carefully)
    , @StatsEvery     int            = 5;                                       -- STATS interval in percent

/********************************************
 DO NOT EDIT BELOW THIS LINE UNLESS NEEDED
*********************************************/
SET NOCOUNT ON;

DECLARE
      @BackupBaseName     nvarchar(200)
    , @DbBackupPath       nvarchar(260)
    , @TimeStamp          nvarchar(16)
    , @BackupSetName      nvarchar(200)
    , @BackupSetDesc      nvarchar(400)
    , @BackupCommand      nvarchar(MAX)
    , @VerifyCommand      nvarchar(MAX)
    , @CRLF               nchar(2)       = NCHAR(13) + NCHAR(10)
    , @i                  int
    , @FileName           nvarchar(400)
    , @Msg                nvarchar(4000);

/********************************************
 1. Basic validation
*********************************************/
IF DB_ID(@DbName) IS NULL
BEGIN
    SET @Msg = N'ERROR: Database [' + ISNULL(@DbName, N'(NULL)') + N'] does not exist.';
    RAISERROR(@Msg, 16, 1);
    RETURN;
END;

IF @StripeCount IS NULL OR @StripeCount < 1
BEGIN
    RAISERROR('ERROR: @StripeCount must be >= 1.', 16, 1);
    RETURN;
END;

-- Basic check that backup path is not NULL/empty
IF (@BackupPath IS NULL OR LTRIM(RTRIM(@BackupPath)) = N'')
BEGIN
    RAISERROR('ERROR: @BackupPath must be specified.', 16, 1);
    RETURN;
END;

-- Ensure trailing backslash
IF RIGHT(@BackupPath, 1) NOT IN ('\', '/')
BEGIN
    SET @BackupPath = @BackupPath + N'\';
END;

-- Check that base backup directory exists using xp_fileexist
DECLARE @DirCheck TABLE (FileExists int, IsDir int, ParentDirExists int);
INSERT INTO @DirCheck
EXEC master.dbo.xp_fileexist @BackupPath;

IF NOT EXISTS (SELECT 1 FROM @DirCheck WHERE IsDir = 1)
BEGIN
    SET @Msg = N'ERROR: Backup directory not found or not accessible: ' + @BackupPath;
    RAISERROR(@Msg, 16, 1);
    RETURN;
END;

-- Derive per-database backup path and create it if needed (e.g. D:\SQLBackups\YourDatabaseName\)
SET @DbBackupPath = @BackupPath + @DbName + N'\';

DECLARE @DbDirCheck TABLE (FileExists int, IsDir int, ParentDirExists int);
INSERT INTO @DbDirCheck
EXEC master.dbo.xp_fileexist @DbBackupPath;

IF NOT EXISTS (SELECT 1 FROM @DbDirCheck WHERE IsDir = 1)
BEGIN
    -- Attempt to create the per-database directory
    EXEC master.dbo.xp_create_subdir @DbBackupPath;

    -- Re-check that it now exists
    DELETE FROM @DbDirCheck;
    INSERT INTO @DbDirCheck
    EXEC master.dbo.xp_fileexist @DbBackupPath;

    IF NOT EXISTS (SELECT 1 FROM @DbDirCheck WHERE IsDir = 1)
    BEGIN
        SET @Msg = N'ERROR: Failed to create per-database backup directory: ' + @DbBackupPath;
        RAISERROR(@Msg, 16, 1);
        RETURN;
    END;
END;

/********************************************
 2. Derive base names for this backup
*********************************************/
SET @TimeStamp      = CONVERT(nvarchar(8),  GETDATE(), 112) + N'_' +
                      REPLACE(CONVERT(nvarchar(5), GETDATE(), 108), N':', N''); -- yyyyMMdd_HHmm
SET @BackupBaseName = @DbName + N'_' + @TimeStamp + N'_FULL';
SET @BackupSetName  = @DbName + N' FULL backup ' + @TimeStamp;
SET @BackupSetDesc  = N'FULL database backup of [' + @DbName + N'] on ' + CONVERT(nvarchar(19), GETDATE(), 120);

PRINT 'Backing up database [' + @DbName + '] to ' + CAST(@StripeCount AS nvarchar(10)) +
      ' backup file(s) in path: ' + @DbBackupPath;
PRINT 'Backup base name: ' + @BackupBaseName;

/********************************************
 3. Build multi-file BACKUP DATABASE command
*********************************************/
SET @BackupCommand = N'BACKUP DATABASE ' + QUOTENAME(@DbName) + @CRLF +
                     N'TO ';

SET @i = 1;
WHILE @i <= @StripeCount
BEGIN
    SET @FileName = @DbBackupPath
                  + @BackupBaseName
                  + N'_' + RIGHT('00' + CAST(@i AS nvarchar(2)), 2) + N'.bak';

    SET @BackupCommand = @BackupCommand +
        CASE WHEN @i > 1 THEN N',' + @CRLF + N'   ' ELSE N'   ' END +
        N'DISK = N''' + @FileName + N'''';

    SET @i += 1;
END;

SET @BackupCommand = @BackupCommand + @CRLF +
    N'WITH ' +
    N'NAME = N''' + @BackupSetName + N''', ' +
    N'DESCRIPTION = N''' + @BackupSetDesc + N''', ' +
    CASE WHEN @UseCompression = 1 THEN N'COMPRESSION, ' ELSE N'' END +
    CASE WHEN @UseChecksum    = 1 THEN N'CHECKSUM, '    ELSE N'' END +
    CASE WHEN @Overwrite      = 1 THEN N'INIT, '        ELSE N'' END +
    CASE WHEN @UseFormat      = 1 THEN N'FORMAT, '      ELSE N'' END +
    N'STATS = ' + CAST(@StatsEvery AS nvarchar(10)) + N';';

PRINT '--------------------------------------------------------------------------------';
PRINT 'BACKUP command to be executed:';
PRINT @BackupCommand;
PRINT '--------------------------------------------------------------------------------';

BEGIN TRY
    PRINT 'Starting BACKUP DATABASE...';
    EXEC (@BackupCommand);
    PRINT 'BACKUP DATABASE completed successfully.';
END TRY
BEGIN CATCH
    SET @Msg = N'ERROR during BACKUP DATABASE. Number: ' + CAST(ERROR_NUMBER() AS nvarchar(10)) +
               N', Severity: ' + CAST(ERROR_SEVERITY() AS nvarchar(10)) +
               N', State: ' + CAST(ERROR_STATE() AS nvarchar(10)) +
               N', Procedure: ' + ISNULL(ERROR_PROCEDURE(), N'(none)') +
               N', Line: ' + CAST(ERROR_LINE() AS nvarchar(10)) +
               N', Message: ' + ERROR_MESSAGE();
    RAISERROR(@Msg, 16, 1);
    RETURN;
END CATCH;

/********************************************
 4. Build & run RESTORE VERIFYONLY command
*********************************************/
SET @VerifyCommand = N'RESTORE VERIFYONLY' + @CRLF +
                     N'FROM ';

SET @i = 1;
WHILE @i <= @StripeCount
BEGIN
    SET @FileName = @DbBackupPath
                  + @BackupBaseName
                  + N'_' + RIGHT('00' + CAST(@i AS nvarchar(2)), 2) + N'.bak';

    SET @VerifyCommand = @VerifyCommand +
        CASE WHEN @i > 1 THEN N',' + @CRLF + N'     ' ELSE N'     ' END +
        N'DISK = N''' + @FileName + N'''';

    SET @i += 1;
END;

SET @VerifyCommand = @VerifyCommand + N';';

PRINT '--------------------------------------------------------------------------------';
PRINT 'VERIFYONLY command to be executed:';
PRINT @VerifyCommand;
PRINT '--------------------------------------------------------------------------------';

BEGIN TRY
    PRINT 'Starting RESTORE VERIFYONLY...';
    EXEC (@VerifyCommand);
    PRINT 'RESTORE VERIFYONLY completed successfully.';
END TRY
BEGIN CATCH
    SET @Msg = N'ERROR during RESTORE VERIFYONLY. Number: ' + CAST(ERROR_NUMBER() AS nvarchar(10)) +
               N', Severity: ' + CAST(ERROR_SEVERITY() AS nvarchar(10)) +
               N', State: ' + CAST(ERROR_STATE() AS nvarchar(10)) +
               N', Procedure: ' + ISNULL(ERROR_PROCEDURE(), N'(none)') +
               N', Line: ' + CAST(ERROR_LINE() AS nvarchar(10)) +
               N', Message: ' + ERROR_MESSAGE();
    RAISERROR(@Msg, 16, 1);
    RETURN;
END CATCH;

/********************************************
 5. RESTORE DATABASE examples (for reference)
*********************************************/
PRINT '--------------------------------------------------------------------------------';
PRINT 'RESTORE DATABASE example (update file paths as needed):';
PRINT '/*';
PRINT 'RESTORE DATABASE [' + @DbName + ']';
PRINT 'FROM';

SET @i = 1;
WHILE @i <= @StripeCount
BEGIN
    SET @FileName = @DbBackupPath
                  + @BackupBaseName
                  + N'_' + RIGHT('00' + CAST(@i AS nvarchar(2)), 2) + N'.bak';

    PRINT CASE WHEN @i > 1 THEN ',' ELSE '    ' END +
          'DISK = N''' + @FileName + N'''';

    SET @i += 1;
END;

PRINT 'WITH';
PRINT '    REPLACE,';  -- remove REPLACE for safety if restoring over an existing DB
PRINT '    RECOVERY;';
PRINT '*/';

PRINT '--------------------------------------------------------------------------------';
PRINT 'If restoring to a new server or different file locations, you may need MOVE clauses,';
PRINT 'for example:';
PRINT '/*';
PRINT 'RESTORE DATABASE [NewDbName]';
PRINT 'FROM';

SET @i = 1;
WHILE @i <= @StripeCount
BEGIN
    SET @FileName = @DbBackupPath
                  + @BackupBaseName
                  + N'_' + RIGHT('00' + CAST(@i AS nvarchar(2)), 2) + N'.bak';

    PRINT CASE WHEN @i > 1 THEN ',' ELSE '    ' END +
          'DISK = N''' + @FileName + N'''';

    SET @i += 1;
END;

PRINT 'WITH';
PRINT '    MOVE ''YourDb_Data'' TO ''D:\SQLData\YourDb_Data.mdf'',';
PRINT '    MOVE ''YourDb_Log''  TO ''D:\SQLLogs\YourDb_Log.ldf'',';
PRINT '    RECOVERY;';
PRINT '*/';
PRINT '--------------------------------------------------------------------------------';

