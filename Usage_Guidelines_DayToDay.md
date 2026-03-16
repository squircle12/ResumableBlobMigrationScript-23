### How to Run the Blob Delta Jobs

## Overview
The Blob Delta Jobs service is a system that allows you to run extracts of blob data from WCCIS at scale. It is designed to be used in conjunction with the Blob Delta Jobs engine. The purpose of this document is to provide an at a glance guide on how to run the service for individual LA's.

## One Time Setup of the FileTable Database
# Step 1: Create the LA Filetable Databases

- Create the LA Filetable Databases in the SQL Server instance. by running the contents of the 03_CreateLAFiletableDatabases.sql script. ensure you replace the LANameHere placeholder with the actual LA name.
Example: 
```
DECLARE @TargetDatabase         sysname        = N'LANameHere_LA_FileTable';  -- The database that will host the FILETABLEs
```
This should be the only line you need to change on the script.
- Run the script

# Step 2: Import the BU's
- The LA_BUs table needs to be populated with the correct Business Units for the given local authority. These will arrive through the freshdesk ticket system and we will create the one off scripts and will be checked into the repository. You can find these scripts in the `BU Import Scripts` folder
- Run the script

# Step 3: Load the Config Values into the Management DB
- The config values can be loaded into the Blob Delta Jobs database using the `04_BlobDeltaJobs_Seed_Config.sql` script, changing the following variable to the relevant LA Filetable database:
```
DECLARE @FileTableDatabase sysname = N'LANameHere_LA_FileTable';   -- Target FileTable DB (e.g. per BU/tenant)
```
## Running the Extraction 
# Overview
The high level approach we are working with here is we do a *"Full"* extraction, which is extract everything from the year dot to present, and then once that has been sent to the supplier, we can then clear down the filetables, and run again as a *"Delta"* extraction. The difference between a Full and a Delta is *waterlines*.

Basically, every time we run an extract on a database the "High Water" level is set to the date of the extraction. When we re-run the extraction in Delta mode, it looks at the water level and says "send me everything _since_ that date"

# Step 1: Run the full extract
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


