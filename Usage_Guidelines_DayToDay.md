# How to Run the Blob Delta Jobs

## Overview

The Blob Delta Jobs service is a system that allows you to run extracts of blob data from WCCIS at scale. It is designed to be used in conjunction with the Blob Delta Jobs engine. The purpose of this document is to provide an at a glance guide on how to run the service for individual LA's.

## One Time Setup of the FileTable Database

### Step 1: Create the LA Filetable Databases

- Create the LA Filetable Databases in the SQL Server instance. by running the contents of the 03_CreateLAFiletableDatabases.sql script. ensure you replace the LANameHere placeholder with the actual LA name.
Example:

```
DECLARE @TargetDatabase         sysname        = N'LANameHere_LA_FileTable';  -- The database that will host the FILETABLEs
```

This should be the only line you need to change on the script.

- Run the script

### Step 2: Import the BU's

- The LA_BUs table needs to be populated with the correct Business Units for the given local authority. These will arrive through the freshdesk ticket system and we will create the one off scripts and will be checked into the repository. You can find these scripts in the `BU Import Scripts` folder
- Run the script

### Step 3: Load the Config Values into the Management DB

- The config values can be loaded into the Blob Delta Jobs database using the `04_BlobDeltaJobs_Seed_Config.sql` script, changing the following variable to the relevant LA Filetable database:

```
DECLARE @FileTableDatabase sysname = N'LANameHere_LA_FileTable';   -- Target FileTable DB (e.g. per BU/tenant)
```

## Running the Extraction

### Overview

The high level approach we are working with here is we do a *"Full"* extraction, which is extract everything from the year dot to present, and then once that has been sent to the supplier, we can then clear down the filetables, and run again as a *"Delta"* extraction. The difference between a Full and a Delta is *waterlines*.

Basically, every time we run an extract on a database the "High Water" level is set to the date of the extraction. When we re-run the extraction in Delta mode, it looks at the water level and says "send me everything *since* that date"

When the extraction is complete, we run a backup of the database with the supplied backup script and we then truncate the file tables to preserve resources.

### Step 1: Run the full extract

The simplest approach to run the extract is to run a Full extract for a given database so the basic syntax is:

```
USE [BlobDeltaJobs]
GO

DECLARE	@return_value int,
		@RunId uniqueidentifier

EXEC	@return_value = [dbo].[usp_BlobDelta_Run]
		@RunId = @RunId OUTPUT,
		@RunType = N'''Full''',
		@TargetDatabase = 'LANameHere_LA_FileTable'

SELECT	@RunId as N'@RunId'

SELECT	'Return Value' = @return_value

GO
```

There are other parameters etc, but the basics are set the `RunType` to `'Full'`, and set the `TargetDatabase` to the LA FileTable DB Name. Progress can be monitored in the Messages tab of SSMS as well as the `dbo.BlobDeltaRunStep` table.

**TIP:** If a Full run fails to complete due to system issues, by its nature, the script is resumable, so re-executing the "Full" extract it will resume where it left off.

#### Parameters in the stored procedure

`**@RunType` and `@TargetDatabase` parameters**

The core stored procedure for extraction, `[dbo].[usp_BlobDelta_Run]`, uses several key parameters, but the two main ones you'll most often work with are `@RunType` and `@TargetDatabase`:

---

### **@RunType**

- **Purpose**:  
Determines the type of extraction you want to perform.
- **Options**:  
  - `'Full'` &mdash; Runs a full extract, bringing all eligible data into the target database (and resetting the "high water mark" for delta runs).
  - `'Delta'` &mdash; Extracts only new or changed records since the last run, according to the high water mark.
  - `'DryRun'` &mdash; Prints the dynamic SQL statements that would be executed, but *does not* modify or insert any data (useful for debugging).
- **Typical Values**:  
`'Full'`, `'Delta'`, `'DryRun'`  
(Can also be passed as `N'Full'` etc. for explicit Unicode.)

---

### **@TargetDatabase**

- **Purpose**:  
Specifies which FileTable database (e.g. for the relevant Local Authority/BU) you want to extract data *into*.
- **Required/Optional**:  
Typically **required** for most extracts.  
If omitted or set to `NULL`, the engine will use active entries from the config table (`BlobDeltaTargetDatabases` with `Extract=1`) to determine which databases to process (usually not needed for day-to-day ops).
- **Example Value**:  
`'Gwent_LA_FileTable'`, `'YnysMon_LA_FileTable'`, `'Wrexham_LA_FileTable'` (use the actual database name for your LA)

---

### **Examples**

**Run a Full Extract for Gwent:**

```
DECLARE @return_value int, @RunId uniqueidentifier

EXEC @return_value = [dbo].[usp_BlobDelta_Run]
    @RunId = @RunId OUTPUT,
    @RunType = N'Full',
    @TargetDatabase = N'Gwent_LA_FileTable'

SELECT @RunId as N'@RunId'
SELECT 'Return Value' = @return_value
```

**Run a Delta Extract for Ynys Mon:**

```
DECLARE @return_value int, @RunId uniqueidentifier

EXEC @return_value = [dbo].[usp_BlobDelta_Run]
    @RunId = @RunId OUTPUT,
    @RunType = N'Delta',
    @TargetDatabase = N'YnysMon_LA_FileTable'

SELECT @RunId as N'@RunId'
SELECT 'Return Value' = @return_value
```

**Dry Run for Wrexham to Preview SQL (no data modifications):**

```
DECLARE @return_value int, @RunId uniqueidentifier

EXEC @return_value = [dbo].[usp_BlobDelta_Run]
    @RunId = @RunId OUTPUT,
    @RunType = N'DryRun',
    @TargetDatabase = N'Wrexham_LA_FileTable'

SELECT @RunId as N'@RunId'
SELECT 'Return Value' = @return_value
```

---

**Summary Table:**


| Parameter         | Description                                   | Example Value           |
| ----------------- | --------------------------------------------- | ----------------------- |
| `@RunType`        | Extract mode (`Full`, `Delta`, or `DryRun`)   | `N'Full'` / `N'Delta'`  |
| `@TargetDatabase` | Name of the destination LA FileTable database | `N'Gwent_LA_FileTable'` |


---

**Recommendation:**  
For day-to-day use, you'll most often use `@RunType = N'Full'` or `N'Delta'`, and always specify the correct `@TargetDatabase` for your LA's filetable DB.

### Step 2: Backup the Database

Again, the backup script has been provided that simplifies the process to standardised backup settings, splitting the files for ease of transfer, and verifying them after the files have been generated

To back up an LA's FileTable database, use the provided backup script found at [`Misc Scripts/BackupDatabase.sql`](./Misc%20Scripts/BackupDatabase.sql).  
**You only need to change one line:** set the `@DbName` variable at the top of the script to your target FileTable database (e.g., `N'YnysMon_LA_FileTable'`).

Example:
```
DECLARE
      @DbName         sysname        = N'YnysMon_LA_FileTable' -- database to back up
    , @BackupPath     nvarchar(260)  = N'D:\MSSQL\MSSQL15.MSSQLSERVER\MSSQL\'   -- backup file path
    -- etc.
```

- After setting `@DbName`, run the entire script.
- The script handles creating a backup folder, stripes the backup across several files (for faster transfer), and verifies the backup using `RESTORE VERIFYONLY` when finished.
- **No other changes are needed unless you want to adjust backup location or stripe count.**

**Summary:**  
Just update the `@DbName` value at the top of the backup script, then execute the script against your SQL Server instance.

### Step 3: Truncate the Database

In order to manage resources effectively - there will be a need to run the truncate script in order to empty the filetables of the database we have extracted ready for subsequent re-use. Because of timings, this may/may not be necessary - as another database restore of the blob database may necessitate a database refresh anyway (please see Step 1 above). In order to truncate the filetables please see below:

To truncate (i.e., empty) all FILETABLE data for an LA FileTable database, use the provided destructive truncate script at [`Destructive Scripts/DropFileTablesInDatabase.sql`](./Destructive%20Scripts/DropFileTablesInDatabase.sql).

**Usage:**

- **Only one line to change:** At the very top of the script, set the `@TargetDatabase` variable to the name of the database you want to empty, e.g.:
  ```
  DECLARE @TargetDatabase sysname = N'YnysMon_LA_FileTable'; -- <- Change this to your LA FileTable DB
  ```
- After setting `@TargetDatabase`, run the script in SQL Server Management Studio (SSMS) or your preferred SQL environment.

**What this script does:**
- Deletes all rows from every FILETABLE in the target database (excluding the implicit root folder row).
- Attempts to force FILESTREAM garbage collection in the database. This triggers cleanup of deleted blob files on disk.

**Important Notes:**
- **Data is unrecoverable after this operation.** Ensure backups are taken if required.
- **Space is not immediately freed.** While the script forces FILESTREAM garbage collection, Windows may not immediately reclaim disk space in all scenarios. You might observe freed space with a short delay, depending on the SQL Server version and system activity.
- No other changes are needed in the script unless you want to change advanced options.

**Summary:**  
To safely and completely empty an LA's FileTable database for re-use, simply set the `@TargetDatabase` variable to your database name at the top of the script, then execute it. Allow for some time for disk space to be reclaimed due to the way FILESTREAM garbage collection behaves.


