-- =============================================================================
-- Blob Delta Jobs: Views and Error Handling Test Harness
-- -----------------------------------------------------------------------------
-- Purpose
--   - Provide a summary view over Run, RunStep, and
--     ErrorLog to make it easier to inspect run outcomes, including
--     non-fatal errors.
--   - Provide a lightweight test harness to exercise the new error handling
--     behaviour for usp_BlobDelta_Run.
-- =============================================================================

USE BlobDeltaJobs;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

-- -----------------------------------------------------------------------------
-- 1. Run summary view
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.vw_RunSummary', N'V') IS NOT NULL
    DROP VIEW dbo.vw_RunSummary;
GO

CREATE VIEW dbo.vw_RunSummary
AS
SELECT
    r.RunId,
    r.RunType,
    r.RequestedBy,
    r.RunStartedAt,
    r.RunCompletedAt,
    r.Status,
    r.ErrorMessage,
    NonFatalErrorCount = ISNULL((
        SELECT COUNT(1)
        FROM dbo.ErrorLog el
        WHERE el.RunId = r.RunId
    ), 0),
    FailedSteps = ISNULL((
        SELECT COUNT(1)
        FROM dbo.RunStep s
        WHERE s.RunId = r.RunId
          AND s.Status IN (N'Failed', N'FailedNonFatal', N'Error')
    ), 0)
FROM dbo.Run r;
GO

-- -----------------------------------------------------------------------------
-- 2. Error handling test harness (manual execution)
-- -----------------------------------------------------------------------------

-- This section is intended to be run manually in a lower environment to
-- validate error handling behaviour. It does not run automatically.
--
-- Example usage patterns:
--
--   1. Non-fatal batch error simulation:
--      - Create a temporary test entry in TableConfig that points to
--        a small source/target pair where you can safely provoke a constraint
--        or data error inside the dynamic script bodies.
--      - Add a corresponding ErrorPolicy row with IsFatal = 0 for the
--        chosen ErrorNumber (and optionally ErrorMessagePattern).
--      - Execute:
--            EXEC dbo.usp_BlobDelta_RunOperator
--                @Mode          = N'SingleTable',
--                @TableName     = N'<YourTestTableName>',
--                @BatchSize     = 10,
--                @MaxDOP        = 1,
--                @DryRun        = 0,
--                @TargetDatabase = NULL;
--      - Inspect:
--            SELECT * FROM dbo.ErrorLog WHERE RunId = <RunId>;
--            SELECT * FROM dbo.RunStep   WHERE RunId = <RunId>;
--            SELECT * FROM dbo.vw_RunSummary WHERE RunId = <RunId>;
--
--   2. Fatal schema/config error regression:
--      - Intentionally misconfigure a test table (e.g. incorrect metadata
--        table name) to trigger error 207 or 208, or one of the seeded
--        RAISERROR-based messages.
--      - Execute the same operator call and confirm:
--            - dbo.Run.Status = 'Failed'
--            - Appropriate error rows exist in dbo.ErrorLog.
--            - Remaining tables do not run after the fatal error.
--
-- No automatic data changes are made here; this script only defines the view
-- and documents manual test steps.

