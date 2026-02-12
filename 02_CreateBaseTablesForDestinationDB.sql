-- 02_CreateBaseTablesForDestinationDB.sql
-- Purpose:
--   - Create the database with FILESTREAM filegroup configured (if it doesn't exist).
--   - Create the base lookup table and FILETABLEs required by the Blob Delta load process.
--   - LA_BU: maps business units to readable names and organisations.
--   - ReferralAttachment, ClientAttachment, Documents: FILETABLEs holding the physical blobs.
--
-- Idempotency:
--   - The database is only created if it does not already exist.
--   - FILESTREAM options are set if not already configured.
--   - The FILESTREAM file is added only if the filegroup exists but has no file.
--   - Each table is only created if it does not already exist in the target database.
--   - This script can be safely re-run without failing on existing objects.
--
-- IMPORTANT:
--   - Update the configuration variables below (@TargetDatabase, @DataFilePath, @LogFilePath, @FileStreamPath)
--     to match your environment before running this script.
--   - Run this script on the SQL Server instance that will host the FILETABLE data and where
--     FILESTREAM is already enabled at the instance level.

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

-- ============================================================================
-- CONFIGURATION VARIABLES - Update these for your environment
-- ============================================================================
DECLARE @TargetDatabase         sysname        = N'YnysMon_LA_FileTable';  -- The database that will host the FILETABLEs
DECLARE @DataFilePath           nvarchar(260)  = N'H:\MSSQL\MSSQL15.MSSQLSERVER\MSSQL\DATA';  -- Path for database data files (.mdf)
DECLARE @LogFilePath            nvarchar(260)  = N'E:\MSSQL\MSSQL15.MSSQLSERVER\MSSQL\Log';     -- Path for database log files (.ldf)
DECLARE @FileStreamPath         nvarchar(260)  = N'F:\MSSQL\MSSQL15.MSSQLSERVER\MSSQL\FileStream\' + @TargetDatabase;  -- Path for FILESTREAM data
DECLARE @FileTableDirectoryName nvarchar(128)  = @TargetDatabase;  -- FILESTREAM DIRECTORY_NAME (must be non-NULL and unique per instance)

DECLARE @Sql                    nvarchar(max);
DECLARE @DataFileName           nvarchar(260)  = @DataFilePath + N'\' + @TargetDatabase + N'.mdf';
DECLARE @LogFileName            nvarchar(260)  = @LogFilePath + N'\' + @TargetDatabase + N'_log.ldf';

-------------------------------------------------------------------------------
-- Step 1: Create the database with FILESTREAM filegroup if it doesn't exist
-------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @TargetDatabase)
BEGIN
    PRINT 'Creating database ' + QUOTENAME(@TargetDatabase) + ' with FILESTREAM filegroup...';
    
    SET @Sql = N'CREATE DATABASE ' + QUOTENAME(@TargetDatabase) + N'
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N''' + @TargetDatabase + N''', FILENAME = N''' + @DataFileName + N''' , SIZE = 8192KB , FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N''' + @TargetDatabase + N'_log'', FILENAME = N''' + @LogFileName + N''' , SIZE = 8192KB , FILEGROWTH = 65536KB );
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' ADD FILEGROUP [FileStreamGroup] CONTAINS FILESTREAM;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET COMPATIBILITY_LEVEL = 150;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET ANSI_NULL_DEFAULT OFF;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET ANSI_NULLS OFF;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET ANSI_PADDING OFF;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET ANSI_WARNINGS OFF;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET ARITHABORT OFF;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET AUTO_CLOSE OFF;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET AUTO_SHRINK OFF;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET AUTO_CREATE_STATISTICS ON(INCREMENTAL = OFF);
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET AUTO_UPDATE_STATISTICS ON;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET CURSOR_CLOSE_ON_COMMIT OFF;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET CURSOR_DEFAULT  GLOBAL;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET CONCAT_NULL_YIELDS_NULL OFF;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET NUMERIC_ROUNDABORT OFF;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET QUOTED_IDENTIFIER OFF;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET RECURSIVE_TRIGGERS OFF;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET  DISABLE_BROKER;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET AUTO_UPDATE_STATISTICS_ASYNC OFF;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET DATE_CORRELATION_OPTIMIZATION OFF;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET PARAMETERIZATION SIMPLE;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET READ_COMMITTED_SNAPSHOT OFF;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET  READ_WRITE;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET RECOVERY FULL;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET  MULTI_USER;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET PAGE_VERIFY CHECKSUM;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET TARGET_RECOVERY_TIME = 60 SECONDS;
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' SET DELAYED_DURABILITY = DISABLED;';
    
    EXEC (@Sql);
    
    -- Set default filegroup
    SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (SELECT name FROM sys.filegroups WHERE is_default=1 AND name = N''PRIMARY'') 
    ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' MODIFY FILEGROUP [PRIMARY] DEFAULT;';
    EXEC (@Sql);
END
ELSE
BEGIN
    PRINT 'Database ' + QUOTENAME(@TargetDatabase) + ' already exists. Skipping database creation.';
    
    -- Ensure FileStreamGroup exists if database already existed
    SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.filegroups
    WHERE name = N''FileStreamGroup''
)
BEGIN
    ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N'
    ADD FILEGROUP [FileStreamGroup] CONTAINS FILESTREAM;
END;';
    EXEC (@Sql);
END;

-------------------------------------------------------------------------------
-- Step 2: Add FILESTREAM file to FileStreamGroup if it doesn't exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.database_files df
    JOIN sys.filegroups fg ON df.data_space_id = fg.data_space_id
    WHERE fg.name = N''FileStreamGroup''
)
BEGIN
    PRINT ''Adding FILESTREAM file to [FileStreamGroup] in database ' + @TargetDatabase + N'...'';
    ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N'
    ADD FILE
    (
        NAME = N''' + @TargetDatabase + N'_FS'',
        FILENAME = N''' + @FileStreamPath + N'''
    )
    TO FILEGROUP [FileStreamGroup];
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 3: Set FILESTREAM database options (DIRECTORY_NAME, NON_TRANSACTED_ACCESS)
-- This is required before creating FileTables.
-------------------------------------------------------------------------------
SET @Sql = N'PRINT ''Setting FILESTREAM options (DIRECTORY_NAME, NON_TRANSACTED_ACCESS) for database ' + @TargetDatabase + N'...''; 
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N'
SET FILESTREAM
(
    DIRECTORY_NAME        = N''' + @FileTableDirectoryName + N''',
    NON_TRANSACTED_ACCESS = FULL
);';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 4: Create the LA_BU lookup table if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''LA_BU''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating table [dbo].[LA_BU]...'';
    CREATE TABLE [dbo].[LA_BU](
        [businessunit] [varchar](50) NOT NULL,
        [BU_Name] [varchar](100) NOT NULL,
        [Organisation] [varchar](50) NOT NULL
    ) ON [PRIMARY];
END
ELSE
BEGIN
    PRINT ''Table [dbo].[LA_BU] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 5: Create the ReferralAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''ReferralAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[ReferralAttachment]...'';
    CREATE TABLE [dbo].[ReferralAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''ReferralAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[ReferralAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 6: Create the ClientAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''ClientAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[ClientAttachment]...'';
    CREATE TABLE [dbo].[ClientAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''ClientAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[ClientAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 7: Create the Documents FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''Documents''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[Documents]...'';
    CREATE TABLE [dbo].[Documents] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''Documents'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[Documents] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

PRINT 'Script completed successfully for database ' + QUOTENAME(@TargetDatabase) + '.';
