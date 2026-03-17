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
DECLARE @TargetDatabase         sysname        = N'LANameHere_LA_FileTable';  -- The database that will host the FILETABLEs
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
--         Force SINGLE_USER temporarily to avoid blocking
-------------------------------------------------------------------------------
SET @Sql = N'
IF EXISTS (
    SELECT 1
    FROM sys.database_filestream_options
    WHERE DB_NAME(database_id) = N''' + @TargetDatabase + N'''
      AND directory_name = N''' + @FileTableDirectoryName + N'''
      AND non_transacted_access_desc = ''FULL''
)
BEGIN
    PRINT ''FILESTREAM options already configured for database ' + @TargetDatabase + N'. Skipping FILESTREAM configuration.'';
END
ELSE
BEGIN
    PRINT ''Setting FILESTREAM options (DIRECTORY_NAME, NON_TRANSACTED_ACCESS) for database ' + @TargetDatabase + N'...'';
    ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N'
    SET FILESTREAM
    (
        DIRECTORY_NAME        = N''' + @FileTableDirectoryName + N''',
        NON_TRANSACTED_ACCESS = FULL
    );
END;';

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
-- Step 4a: Ensure LA_BU.businessunit is unique via index
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''LA_BU''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
AND NOT EXISTS (
    SELECT 1
    FROM sys.indexes i
    WHERE i.name = N''IX_LA_BU_businessunit''
      AND i.object_id = OBJECT_ID(N''dbo.LA_BU'')
)
BEGIN
    PRINT ''Creating unique index [IX_LA_BU_businessunit] on [dbo].[LA_BU]([businessunit])...'';
    CREATE UNIQUE NONCLUSTERED INDEX [IX_LA_BU_businessunit]
    ON [dbo].[LA_BU]([businessunit]);
END
ELSE IF EXISTS (
    SELECT 1
    FROM sys.indexes i
    WHERE i.name = N''IX_LA_BU_businessunit''
      AND i.object_id = OBJECT_ID(N''dbo.LA_BU'')
)
BEGIN
    PRINT ''Unique index [IX_LA_BU_businessunit] on [dbo].[LA_BU] already exists. Skipping creation.'';
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

-------------------------------------------------------------------------------
-- Step 8: Create the ProviderAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''ProviderAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[ProviderAttachment]...'';
    CREATE TABLE [dbo].[ProviderAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''ProviderAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[ProviderAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 9: Create the ReferralFormAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''ReferralFormAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[ReferralFormAttachment]...'';
    CREATE TABLE [dbo].[ReferralFormAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''ReferralFormAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[ReferralFormAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 10: Create the AllergyAndReactionAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''AllergyAndReactionAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[AllergyAndReactionAttachment]...'';
    CREATE TABLE [dbo].[AllergyAndReactionAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''AllergyAndReactionAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[AllergyAndReactionAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 11: Create the AssessmentPrintRecord FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''AssessmentPrintRecord''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[AssessmentPrintRecord]...'';
    CREATE TABLE [dbo].[AssessmentPrintRecord] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''AssessmentPrintRecord'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[AssessmentPrintRecord] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 12: Create the ClientPortabilityAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''ClientPortabilityAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[ClientPortabilityAttachment]...'';
    CREATE TABLE [dbo].[ClientPortabilityAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''ClientPortabilityAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[ClientPortabilityAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 13: Create the ClinicAppointmentAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''ClinicAppointmentAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[ClinicAppointmentAttachment]...'';
    CREATE TABLE [dbo].[ClinicAppointmentAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''ClinicAppointmentAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[ClinicAppointmentAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 14: Create the ConsentToTreatmentAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''ConsentToTreatmentAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[ConsentToTreatmentAttachment]...'';
    CREATE TABLE [dbo].[ConsentToTreatmentAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''ConsentToTreatmentAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[ConsentToTreatmentAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 15: Create the CourtDatesAndOutcomesAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''CourtDatesAndOutcomesAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[CourtDatesAndOutcomesAttachment]...'';
    CREATE TABLE [dbo].[CourtDatesAndOutcomesAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''CourtDatesAndOutcomesAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[CourtDatesAndOutcomesAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 16: Create the FamilyFormAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''FamilyFormAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[FamilyFormAttachment]...'';
    CREATE TABLE [dbo].[FamilyFormAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''FamilyFormAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[FamilyFormAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 17: Create the FamilyReferralAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''FamilyReferralAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[FamilyReferralAttachment]...'';
    CREATE TABLE [dbo].[FamilyReferralAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''FamilyReferralAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[FamilyReferralAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 18: Create the Genogram FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''Genogram''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[Genogram]...'';
    CREATE TABLE [dbo].[Genogram] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''Genogram'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[Genogram] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 19: Create the Letters FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''Letters''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[Letters]...'';
    CREATE TABLE [dbo].[Letters] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''Letters'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[Letters] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 20: Create the MHALegalStatusAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''MHALegalStatusAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[MHALegalStatusAttachment]...'';
    CREATE TABLE [dbo].[MHALegalStatusAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''MHALegalStatusAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[MHALegalStatusAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 21: Create the MHMFormAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''MHMFormAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[MHMFormAttachment]...'';
    CREATE TABLE [dbo].[MHMFormAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''MHMFormAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[MHMFormAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 22: Create the PersonBodyMapAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''PersonBodyMapAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[PersonBodyMapAttachment]...'';
    CREATE TABLE [dbo].[PersonBodyMapAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''PersonBodyMapAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[PersonBodyMapAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 23: Create the ProviderFormAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''ProviderFormAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[ProviderFormAttachment]...'';
    CREATE TABLE [dbo].[ProviderFormAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''ProviderFormAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[ProviderFormAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 24: Create the RecordOfAppealAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''RecordOfAppealAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[RecordOfAppealAttachment]...'';
    CREATE TABLE [dbo].[RecordOfAppealAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''RecordOfAppealAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[RecordOfAppealAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 25: Create the ReferralFormActivityAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''ReferralFormActivityAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[ReferralFormActivityAttachment]...'';
    CREATE TABLE [dbo].[ReferralFormActivityAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''ReferralFormActivityAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[ReferralFormActivityAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 26: Create the ReferralFormHistory FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''ReferralFormHistory''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[ReferralFormHistory]...'';
    CREATE TABLE [dbo].[ReferralFormHistory] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''ReferralFormHistory'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[ReferralFormHistory] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 27: Create the ReportsAndFormsActivityAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''ReportsAndFormsActivityAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[ReportsAndFormsActivityAttachment]...'';
    CREATE TABLE [dbo].[ReportsAndFormsActivityAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''ReportsAndFormsActivityAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[ReportsAndFormsActivityAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 28: Create the SARDocument FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''SARDocument''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[SARDocument]...'';
    CREATE TABLE [dbo].[SARDocument] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''SARDocument'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[SARDocument] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 29: Create the SARTemplate FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''SARTemplate''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[SARTemplate]...'';
    CREATE TABLE [dbo].[SARTemplate] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''SARTemplate'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[SARTemplate] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 30: Create the SeclusionAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''SeclusionAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[SeclusionAttachment]...'';
    CREATE TABLE [dbo].[SeclusionAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''SeclusionAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[SeclusionAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Step 31: Create the Section117EntitlementAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''Section117EntitlementAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    PRINT ''Creating FILETABLE [dbo].[Section117EntitlementAttachment]...'';
    CREATE TABLE [dbo].[Section117EntitlementAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''Section117EntitlementAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END
ELSE
BEGIN
    PRINT ''FILETABLE [dbo].[Section117EntitlementAttachment] already exists. Skipping creation.'';
END;';

EXEC (@Sql);

PRINT 'Script completed successfully for database ' + QUOTENAME(@TargetDatabase) + '.';

-------------------------------------------------------------------------------
-- Step 32: Ensure BlobDeltaTargetDatabases entry exists for this TargetDatabase
--          (insert-only; do not overwrite existing Extract settings)
-------------------------------------------------------------------------------
IF DB_ID(N'BlobDeltaJobs') IS NOT NULL
BEGIN
    PRINT N'Ensuring BlobDeltaTargetDatabases is seeded for ' + QUOTENAME(@TargetDatabase) + N' in BlobDeltaJobs...';

    EXEC (N'USE BlobDeltaJobs;
IF OBJECT_ID(N''dbo.BlobDeltaTargetDatabases'', N''U'') IS NOT NULL
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM dbo.BlobDeltaTargetDatabases td
        WHERE td.TargetDatabase = N''' + REPLACE(@TargetDatabase, '''', '''''') + N'''
    )
    BEGIN
        PRINT N''Seeding BlobDeltaTargetDatabases for ' + REPLACE(QUOTENAME(@TargetDatabase), '''', '''''') + N' with Extract = 1.'';
        INSERT INTO dbo.BlobDeltaTargetDatabases (TargetDatabase, Extract)
        VALUES (N''' + REPLACE(@TargetDatabase, '''', '''''') + N''', 1);
    END
    ELSE
    BEGIN
        PRINT N''BlobDeltaTargetDatabases already has an entry for ' + REPLACE(QUOTENAME(@TargetDatabase), '''', '''''') + N'. Preserving existing Extract setting.'';
    END
END
ELSE
BEGIN
    PRINT N''Warning: dbo.BlobDeltaTargetDatabases does not exist in BlobDeltaJobs. Run the BlobDeltaJobs schema script to create it.'';
END;');
END
ELSE
BEGIN
    PRINT N'Warning: BlobDeltaJobs database does not exist; skipping BlobDeltaTargetDatabases seeding.';
END;
