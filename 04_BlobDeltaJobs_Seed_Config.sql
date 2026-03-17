-- =============================================================================
-- Blob Delta Jobs: Seed Table Config and Script Templates
-- -----------------------------------------------------------------------------
-- Purpose
--   Seed initial configuration and script templates for the BlobDeltaJobs
--   project, using the existing ReferralAttachment / ClientAttachment pattern
--   as a baseline and extending it for delta-window and BU-aware behaviour.
--
--   This script is designed to be idempotent via MERGE statements.
--
-- Prerequisite
--   03_BlobDeltaJobs_Schema.sql has been run and BlobDeltaJobs DB exists.
-- =============================================================================

USE BlobDeltaJobs;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO


-- -----------------------------------------------------------------------------
-- Configuration defaults (adjust and rerun to seed for other databases)
-- -----------------------------------------------------------------------------
DECLARE @FileTableDatabase sysname = N'LANameHere_LA_FileTable';   -- Target FileTable DB (e.g. per BU/tenant)
DECLARE @FileTableSchema   sysname = N'dbo';

DECLARE @ReferralTableName     sysname = N'ReferralAttachment';
DECLARE @ClientTableName       sysname = N'ClientAttachment';
DECLARE @ProviderTableName     sysname = N'ProviderAttachment';
DECLARE @ReferralFormTableName sysname = N'ReferralFormAttachment';

DECLARE @SourceDatabase    sysname = N'AdvancedRBSBlob_WCCIS';
DECLARE @SourceSchema      sysname = N'dbo';

DECLARE @MetadataDatabase             sysname = N'AdvancedRBS_MetaData';
DECLARE @MetadataSchema               sysname = N'dbo';
DECLARE @ReferralMetadataTable        sysname = N'cw_referralattachmentBase';
DECLARE @ReferralMetadataIdColumn     sysname = N'cw_referralattachmentId';
DECLARE @ClientMetadataTable          sysname = N'cw_clientattachmentBase';
DECLARE @ClientMetadataIdColumn       sysname = N'cw_clientattachmentId';
DECLARE @ProviderMetadataTable        sysname = N'cw_ProviderAttachmentBase';
DECLARE @ProviderMetadataIdColumn     sysname = N'cw_ProviderAttachmentId';
DECLARE @ReferralFormMetadataTable    sysname = N'cw_assessmentAttachmentBase';
DECLARE @ReferralFormMetadataIdColumn sysname = N'cw_assessmentAttachmentid';
DECLARE @MetadataModifiedOnColumn     sysname = N'ModifiedOn';

-- Additional attachment-style tables from AdvancedRBSBlob_WCCIS
DECLARE @AllergyAndReactionAttachmentTableName           sysname = N'AllergyAndReactionAttachment';
DECLARE @AllergyAndReactionAttachmentMetadataTable       sysname = N'cw_personallergyattachmentBase';
DECLARE @AllergyAndReactionAttachmentMetadataIdColumn    sysname = N'cw_personallergyattachmentId';

DECLARE @AssessmentPrintRecordTableName                  sysname = N'AssessmentPrintRecord';
DECLARE @AssessmentPrintRecordMetadataTable              sysname = N'cw_AssessmentPrintRecordBase';
DECLARE @AssessmentPrintRecordMetadataIdColumn           sysname = N'cw_AssessmentPrintRecordid';

DECLARE @ClientPortabilityAttachmentTableName            sysname = N'ClientPortabilityAttachment';
DECLARE @ClientPortabilityAttachmentMetadataTable        sysname = N'cw_ClientPortabilityAttachmentBase';
DECLARE @ClientPortabilityAttachmentMetadataIdColumn     sysname = N'cw_ClientPortabilityAttachmentid';

DECLARE @ClinicAppointmentAttachmentTableName            sysname = N'ClinicAppointmentAttachment';
DECLARE @ClinicAppointmentAttachmentMetadataTable        sysname = N'cw_ClinicAppointmentAttachmentBase';
DECLARE @ClinicAppointmentAttachmentMetadataIdColumn     sysname = N'cw_ClinicAppointmentAttachmentid';

DECLARE @ConsentToTreatmentAttachmentTableName           sysname = N'ConsentToTreatmentAttachment';
DECLARE @ConsentToTreatmentAttachmentMetadataTable       sysname = N'cw_ConsentToTreatmentAttachmentBase';
DECLARE @ConsentToTreatmentAttachmentMetadataIdColumn    sysname = N'cw_ConsentToTreatmentAttachmentid';

DECLARE @CourtDatesAndOutcomesAttachmentTableName        sysname = N'CourtDatesAndOutcomesAttachment';
DECLARE @CourtDatesAndOutcomesAttachmentMetadataTable    sysname = N'cw_CourtDatesAndOutcomesAttachmentBase';
DECLARE @CourtDatesAndOutcomesAttachmentMetadataIdColumn sysname = N'cw_CourtDatesAndOutcomesAttachmentid';

DECLARE @FamilyFormAttachmentTableName                   sysname = N'FamilyFormAttachment';
DECLARE @FamilyFormAttachmentMetadataTable               sysname = N'cw_FamilyFormAttachmentBase';
DECLARE @FamilyFormAttachmentMetadataIdColumn            sysname = N'cw_FamilyFormAttachmentid';

DECLARE @FamilyReferralAttachmentTableName               sysname = N'FamilyReferralAttachment';
DECLARE @FamilyReferralAttachmentMetadataTable           sysname = N'cw_FamilyReferralAttachmentBase';
DECLARE @FamilyReferralAttachmentMetadataIdColumn        sysname = N'cw_FamilyReferralAttachmentid';

DECLARE @GenogramTableName                               sysname = N'Genogram';
DECLARE @GenogramMetadataTable                           sysname = N'cw_GenogramBase';
DECLARE @GenogramMetadataIdColumn                        sysname = N'cw_Genogramid';

DECLARE @LettersTableName                                sysname = N'Letters';
DECLARE @LettersMetadataTable                            sysname = N'LetterBase';
DECLARE @LettersMetadataIdColumn                         sysname = N'ActivityId';

DECLARE @MHALegalStatusAttachmentTableName               sysname = N'MHALegalStatusAttachment';
DECLARE @MHALegalStatusAttachmentMetadataTable           sysname = N'cw_MHALegalStatusAttachmentBase';
DECLARE @MHALegalStatusAttachmentMetadataIdColumn        sysname = N'cw_MHALegalStatusAttachmentid';

DECLARE @MHMFormAttachmentTableName                      sysname = N'MHMFormAttachment';
DECLARE @MHMFormAttachmentMetadataTable                  sysname = N'cw_mentalhealthmeasureformattachmentBase';
DECLARE @MHMFormAttachmentMetadataIdColumn               sysname = N'cw_mentalhealthmeasureformattachmentId';

DECLARE @PersonBodyMapAttachmentTableName                sysname = N'PersonBodyMapAttachment';
DECLARE @PersonBodyMapAttachmentMetadataTable            sysname = N'cw_PersonBodyMapAttachmentBase';
DECLARE @PersonBodyMapAttachmentMetadataIdColumn         sysname = N'cw_PersonBodyMapAttachmentid';

DECLARE @ProviderFormAttachmentTableName                 sysname = N'ProviderFormAttachment';
DECLARE @ProviderFormAttachmentMetadataTable             sysname = N'cw_ProviderFormAttachmentBase';
DECLARE @ProviderFormAttachmentMetadataIdColumn          sysname = N'cw_ProviderFormAttachmentid';

DECLARE @RecordOfAppealAttachmentTableName               sysname = N'RecordOfAppealAttachment';
DECLARE @RecordOfAppealAttachmentMetadataTable           sysname = N'cw_RecordOfAppealAttachmentBase';
DECLARE @RecordOfAppealAttachmentMetadataIdColumn        sysname = N'cw_RecordOfAppealAttachmentid';

DECLARE @ReferralFormHistoryTableName                    sysname = N'ReferralFormHistory';
DECLARE @ReferralFormHistoryMetadataTable                sysname = N'cw_AssessmentPrintRecordBase';
DECLARE @ReferralFormHistoryMetadataIdColumn             sysname = N'cw_AssessmentPrintRecordid';

DECLARE @ReportsAndFormsActivityAttachmentTableName      sysname = N'ReportsAndFormsActivityAttachment';
DECLARE @ReportsAndFormsActivityAttachmentMetadataTable  sysname = N'cw_ReportsAndFormsActivityAttachmentBase';
DECLARE @ReportsAndFormsActivityAttachmentMetadataIdColumn sysname = N'cw_ReportsAndFormsActivityAttachmentid';

DECLARE @SARDocumentTableName                            sysname = N'SARDocument';
DECLARE @SARDocumentMetadataTable                        sysname = N'cw_printcasefileBase';
DECLARE @SARDocumentMetadataIdColumn                     sysname = N'cw_printcasefileid';

DECLARE @SARTemplateTableName                            sysname = N'SARTemplate';
DECLARE @SARTemplateMetadataTable                        sysname = N'cw_subjectaccessrequestBase';
DECLARE @SARTemplateMetadataIdColumn                     sysname = N'cw_templateid';

DECLARE @SeclusionAttachmentTableName                    sysname = N'SeclusionAttachment';
DECLARE @SeclusionAttachmentMetadataTable                sysname = N'cw_SeclusionAttachmentBase';
DECLARE @SeclusionAttachmentMetadataIdColumn             sysname = N'cw_SeclusionAttachmentid';

DECLARE @Section117EntitlementAttachmentTableName        sysname = N'Section117EntitlementAttachment';
DECLARE @Section117EntitlementAttachmentMetadataTable    sysname = N'cw_mhasection117entitlementattachmentBase';
DECLARE @Section117EntitlementAttachmentMetadataIdColumn sysname = N'cw_mhasection117entitlementattachmentId';

-- -----------------------------------------------------------------------------
-- 1. Seed BlobDeltaTableConfig for known tables
--    (ReferralAttachment and ClientAttachment initial examples)
-- -----------------------------------------------------------------------------

MERGE dbo.BlobDeltaTableConfig AS t
USING (
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @ReferralTableName AS TableName,
        @SourceDatabase          AS SourceDatabase,  @SourceSchema   AS SourceSchema,  @ReferralTableName  AS SourceTable,
        @FileTableDatabase       AS TargetDatabase,  @FileTableSchema AS TargetSchema, @ReferralTableName  AS TargetTable,
        @MetadataDatabase        AS MetadataDatabase, @MetadataSchema AS MetadataSchema, @ReferralMetadataTable AS MetadataTable,
        @ReferralMetadataIdColumn AS MetadataIdColumn,
        @MetadataModifiedOnColumn AS MetadataModifiedOnCol,
        CAST(240 AS INT)          AS SafetyBufferMinutes,
        CAST(1 AS BIT)            AS IncludeUpdatesInDelta,
        CAST(0 AS BIT)            AS IncludeDeletesInDelta,
        CAST(1 AS BIT)            AS IsActive
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @ClientTableName,
        @SourceDatabase,  @SourceSchema,  @ClientTableName,
        @FileTableDatabase, @FileTableSchema, @ClientTableName,
        @MetadataDatabase, @MetadataSchema, @ClientMetadataTable,
        @ClientMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @ProviderTableName,
        @SourceDatabase,  @SourceSchema,  @ProviderTableName,
        @FileTableDatabase, @FileTableSchema, @ProviderTableName,
        @MetadataDatabase, @MetadataSchema, @ProviderMetadataTable,
        @ProviderMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @ReferralFormTableName,
        @SourceDatabase,  @SourceSchema,  @ReferralFormTableName,
        @FileTableDatabase, @FileTableSchema, @ReferralFormTableName,
        @MetadataDatabase, @MetadataSchema, @ReferralFormMetadataTable,
        @ReferralFormMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @AllergyAndReactionAttachmentTableName,
        @SourceDatabase,  @SourceSchema,  @AllergyAndReactionAttachmentTableName,
        @FileTableDatabase, @FileTableSchema, @AllergyAndReactionAttachmentTableName,
        @MetadataDatabase, @MetadataSchema, @AllergyAndReactionAttachmentMetadataTable,
        @AllergyAndReactionAttachmentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @AssessmentPrintRecordTableName,
        @SourceDatabase,  @SourceSchema,  @AssessmentPrintRecordTableName,
        @FileTableDatabase, @FileTableSchema, @AssessmentPrintRecordTableName,
        @MetadataDatabase, @MetadataSchema, @AssessmentPrintRecordMetadataTable,
        @AssessmentPrintRecordMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(0 AS BIT)          -- Default to not include updates in extraction
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @ClientPortabilityAttachmentTableName,
        @SourceDatabase,  @SourceSchema,  @ClientPortabilityAttachmentTableName,
        @FileTableDatabase, @FileTableSchema, @ClientPortabilityAttachmentTableName,
        @MetadataDatabase, @MetadataSchema, @ClientPortabilityAttachmentMetadataTable,
        @ClientPortabilityAttachmentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @ClinicAppointmentAttachmentTableName,
        @SourceDatabase,  @SourceSchema,  @ClinicAppointmentAttachmentTableName,
        @FileTableDatabase, @FileTableSchema, @ClinicAppointmentAttachmentTableName,
        @MetadataDatabase, @MetadataSchema, @ClinicAppointmentAttachmentMetadataTable,
        @ClinicAppointmentAttachmentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @ConsentToTreatmentAttachmentTableName,
        @SourceDatabase,  @SourceSchema,  @ConsentToTreatmentAttachmentTableName,
        @FileTableDatabase, @FileTableSchema, @ConsentToTreatmentAttachmentTableName,
        @MetadataDatabase, @MetadataSchema, @ConsentToTreatmentAttachmentMetadataTable,
        @ConsentToTreatmentAttachmentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @CourtDatesAndOutcomesAttachmentTableName,
        @SourceDatabase,  @SourceSchema,  @CourtDatesAndOutcomesAttachmentTableName,
        @FileTableDatabase, @FileTableSchema, @CourtDatesAndOutcomesAttachmentTableName,
        @MetadataDatabase, @MetadataSchema, @CourtDatesAndOutcomesAttachmentMetadataTable,
        @CourtDatesAndOutcomesAttachmentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @FamilyFormAttachmentTableName,
        @SourceDatabase,  @SourceSchema,  @FamilyFormAttachmentTableName,
        @FileTableDatabase, @FileTableSchema, @FamilyFormAttachmentTableName,
        @MetadataDatabase, @MetadataSchema, @FamilyFormAttachmentMetadataTable,
        @FamilyFormAttachmentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @FamilyReferralAttachmentTableName,
        @SourceDatabase,  @SourceSchema,  @FamilyReferralAttachmentTableName,
        @FileTableDatabase, @FileTableSchema, @FamilyReferralAttachmentTableName,
        @MetadataDatabase, @MetadataSchema, @FamilyReferralAttachmentMetadataTable,
        @FamilyReferralAttachmentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @GenogramTableName,
        @SourceDatabase,  @SourceSchema,  @GenogramTableName,
        @FileTableDatabase, @FileTableSchema, @GenogramTableName,
        @MetadataDatabase, @MetadataSchema, @GenogramMetadataTable,
        @GenogramMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @LettersTableName,
        @SourceDatabase,  @SourceSchema,  @LettersTableName,
        @FileTableDatabase, @FileTableSchema, @LettersTableName,
        @MetadataDatabase, @MetadataSchema, @LettersMetadataTable,
        @LettersMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(0 AS BIT)          -- Default to not include in extraction
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @MHALegalStatusAttachmentTableName,
        @SourceDatabase,  @SourceSchema,  @MHALegalStatusAttachmentTableName,
        @FileTableDatabase, @FileTableSchema, @MHALegalStatusAttachmentTableName,
        @MetadataDatabase, @MetadataSchema, @MHALegalStatusAttachmentMetadataTable,
        @MHALegalStatusAttachmentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @MHMFormAttachmentTableName,
        @SourceDatabase,  @SourceSchema,  @MHMFormAttachmentTableName,
        @FileTableDatabase, @FileTableSchema, @MHMFormAttachmentTableName,
        @MetadataDatabase, @MetadataSchema, @MHMFormAttachmentMetadataTable,
        @MHMFormAttachmentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @PersonBodyMapAttachmentTableName,
        @SourceDatabase,  @SourceSchema,  @PersonBodyMapAttachmentTableName,
        @FileTableDatabase, @FileTableSchema, @PersonBodyMapAttachmentTableName,
        @MetadataDatabase, @MetadataSchema, @PersonBodyMapAttachmentMetadataTable,
        @PersonBodyMapAttachmentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @ProviderFormAttachmentTableName,
        @SourceDatabase,  @SourceSchema,  @ProviderFormAttachmentTableName,
        @FileTableDatabase, @FileTableSchema, @ProviderFormAttachmentTableName,
        @MetadataDatabase, @MetadataSchema, @ProviderFormAttachmentMetadataTable,
        @ProviderFormAttachmentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @RecordOfAppealAttachmentTableName,
        @SourceDatabase,  @SourceSchema,  @RecordOfAppealAttachmentTableName,
        @FileTableDatabase, @FileTableSchema, @RecordOfAppealAttachmentTableName,
        @MetadataDatabase, @MetadataSchema, @RecordOfAppealAttachmentMetadataTable,
        @RecordOfAppealAttachmentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @ReferralFormHistoryTableName,
        @SourceDatabase,  @SourceSchema,  @ReferralFormHistoryTableName,
        @FileTableDatabase, @FileTableSchema, @ReferralFormHistoryTableName,
        @MetadataDatabase, @MetadataSchema, @ReferralFormHistoryMetadataTable,
        @ReferralFormHistoryMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @ReportsAndFormsActivityAttachmentTableName,
        @SourceDatabase,  @SourceSchema,  @ReportsAndFormsActivityAttachmentTableName,
        @FileTableDatabase, @FileTableSchema, @ReportsAndFormsActivityAttachmentTableName,
        @MetadataDatabase, @MetadataSchema, @ReportsAndFormsActivityAttachmentMetadataTable,
        @ReportsAndFormsActivityAttachmentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @SARDocumentTableName,
        @SourceDatabase,  @SourceSchema,  @SARDocumentTableName,
        @FileTableDatabase, @FileTableSchema, @SARDocumentTableName,
        @MetadataDatabase, @MetadataSchema, @SARDocumentMetadataTable,
        @SARDocumentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(0 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @SARTemplateTableName,
        @SourceDatabase,  @SourceSchema,  @SARTemplateTableName,
        @FileTableDatabase, @FileTableSchema, @SARTemplateTableName,
        @MetadataDatabase, @MetadataSchema, @SARTemplateMetadataTable,
        @SARTemplateMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(0 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @SeclusionAttachmentTableName,
        @SourceDatabase,  @SourceSchema,  @SeclusionAttachmentTableName,
        @FileTableDatabase, @FileTableSchema, @SeclusionAttachmentTableName,
        @MetadataDatabase, @MetadataSchema, @SeclusionAttachmentMetadataTable,
        @SeclusionAttachmentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @Section117EntitlementAttachmentTableName,
        @SourceDatabase,  @SourceSchema,  @Section117EntitlementAttachmentTableName,
        @FileTableDatabase, @FileTableSchema, @Section117EntitlementAttachmentTableName,
        @MetadataDatabase, @MetadataSchema, @Section117EntitlementAttachmentMetadataTable,
        @Section117EntitlementAttachmentMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
) AS s
ON t.TableName = s.TableName
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        TableName,
        SourceDatabase, SourceSchema, SourceTable,
        TargetDatabase, TargetSchema, TargetTable,
        MetadataDatabase, MetadataSchema, MetadataTable, MetadataIdColumn,
        MetadataModifiedOnCol,
        SafetyBufferMinutes,
        IncludeUpdatesInDelta,
        IncludeDeletesInDelta,
        IsActive
    )
    VALUES (
        s.TableName,
        s.SourceDatabase, s.SourceSchema, s.SourceTable,
        s.TargetDatabase, s.TargetSchema, s.TargetTable,
        s.MetadataDatabase, s.MetadataSchema, s.MetadataTable, s.MetadataIdColumn,
        s.MetadataModifiedOnCol,
        s.SafetyBufferMinutes,
        s.IncludeUpdatesInDelta,
        s.IncludeDeletesInDelta,
        s.IsActive
    )
WHEN MATCHED THEN
    UPDATE SET
        SourceDatabase        = s.SourceDatabase,
        SourceSchema          = s.SourceSchema,
        SourceTable           = s.SourceTable,
        TargetDatabase        = s.TargetDatabase,
        TargetSchema          = s.TargetSchema,
        TargetTable           = s.TargetTable,
        MetadataDatabase      = s.MetadataDatabase,
        MetadataSchema        = s.MetadataSchema,
        MetadataTable         = s.MetadataTable,
        MetadataIdColumn      = s.MetadataIdColumn,
        MetadataModifiedOnCol = s.MetadataModifiedOnCol,
        SafetyBufferMinutes   = s.SafetyBufferMinutes,
        IncludeUpdatesInDelta = s.IncludeUpdatesInDelta,
        IncludeDeletesInDelta = s.IncludeDeletesInDelta,
        IsActive              = s.IsActive,
        UpdatedAt             = SYSDATETIME();
GO

-- Ensure matching high-watermark rows exist
MERGE dbo.BlobDeltaHighWatermark AS h
USING (
    SELECT TableName FROM dbo.BlobDeltaTableConfig
) AS s
ON h.TableName = s.TableName
WHEN NOT MATCHED BY TARGET THEN
    INSERT (TableName, LastHighWaterModifiedOn, LastRunId, LastRunCompletedAt,
            IsInitialFullLoadDone, IsRunning, RunLeaseExpiresAt)
    VALUES (s.TableName, NULL, NULL, NULL, 0, 0, NULL);
GO

-- -----------------------------------------------------------------------------
-- 2. Step script templates (Roots, MissingParents batch, Children)
-- -----------------------------------------------------------------------------

-- Step 1: Roots (parent_path_locator IS NULL), delta-windowed and BU-aware.
MERGE dbo.BlobDeltaStepScript AS t
USING (
    SELECT
        CAST(1 AS tinyint)     AS StepNumber,
        N'Roots'               AS ScriptKind,
        N'Roots'               AS StageName,
        CAST(1 AS bit)         AS UseParameterizedMaxDOP,
        N'Roots: insert top batch from source where parent_path_locator IS NULL, delta-windowed by metadata ModifiedOn.' AS Description
) AS s
ON t.StepNumber = s.StepNumber AND t.ScriptKind = s.ScriptKind
WHEN NOT MATCHED BY TARGET THEN
    INSERT (StepNumber, ScriptKind, StageName, ScriptBody, UseParameterizedMaxDOP, Description)
    VALUES (
        s.StepNumber,
        s.ScriptKind,
        s.StageName,
N'
INSERT TOP (@BatchSize) INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],
     [is_archive],[is_system],[is_temporary])
SELECT
    RAFT.[stream_id],
    RAFT.[file_stream],
    RAFT.[name],
    RAFT.[path_locator],
    RAFT.[creation_time],
    RAFT.[last_write_time],
    RAFT.[last_access_time],
    RAFT.[is_directory],
    RAFT.[is_offline],
    RAFT.[is_hidden],
    RAFT.[is_readonly],
    RAFT.[is_archive],
    RAFT.[is_system],
    RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN [MetadataTableFull] RAM WITH (NOLOCK)
    ON RAM.[MetadataIdColumn] = RAFT.stream_id
INNER JOIN [BusinessUnitTableFull] BU WITH (NOLOCK)
    ON BU.businessunit = RAM.OwningBusinessUnit
LEFT JOIN [TargetTableFull] TGT WITH (NOLOCK)
    ON TGT.stream_id = RAFT.stream_id
WHERE RAFT.parent_path_locator IS NULL
  AND TGT.stream_id IS NULL
  AND RAFT.stream_id <> @ExcludedStreamId
  AND RAM.[MetadataModifiedOnColumn] >  @WindowStart
  AND RAM.[MetadataModifiedOnColumn] <= @WindowEnd
--  AND (CASE WHEN @BusinessUnitId IS NULL THEN 1 WHEN BU.businessunit = @BusinessUnitId THEN 1 ELSE 0 END = 1)
ORDER BY RAFT.stream_id
OPTION (MAXDOP [MaxDOP]);
',
        s.UseParameterizedMaxDOP,
        s.Description
    )
WHEN MATCHED THEN
    UPDATE SET
        StageName           = s.StageName,
        ScriptBody          =
N'
INSERT TOP (@BatchSize) INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],
     [is_archive],[is_system],[is_temporary])
SELECT
    RAFT.[stream_id],
    RAFT.[file_stream],
    RAFT.[name],
    RAFT.[path_locator],
    RAFT.[creation_time],
    RAFT.[last_write_time],
    RAFT.[last_access_time],
    RAFT.[is_directory],
    RAFT.[is_offline],
    RAFT.[is_hidden],
    RAFT.[is_readonly],
    RAFT.[is_archive],
    RAFT.[is_system],
    RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN [MetadataTableFull] RAM WITH (NOLOCK)
    ON RAM.[MetadataIdColumn] = RAFT.stream_id
INNER JOIN [BusinessUnitTableFull] BU WITH (NOLOCK)
    ON BU.businessunit = RAM.OwningBusinessUnit
LEFT JOIN [TargetTableFull] TGT WITH (NOLOCK)
    ON TGT.stream_id = RAFT.stream_id
WHERE RAFT.parent_path_locator IS NULL
  AND TGT.stream_id IS NULL
  AND RAFT.stream_id <> @ExcludedStreamId
  AND RAM.[MetadataModifiedOnColumn] >  @WindowStart
  AND RAM.[MetadataModifiedOnColumn] <= @WindowEnd
--  AND (CASE WHEN @BusinessUnitId IS NULL THEN 1 WHEN BU.businessunit = @BusinessUnitId THEN 1 ELSE 0 END = 1)
ORDER BY RAFT.stream_id
OPTION (MAXDOP [MaxDOP]);
',
        UseParameterizedMaxDOP = s.UseParameterizedMaxDOP,
        Description            = s.Description;
GO

-- Step 2: Missing parents batch insert (from #Batch).
MERGE dbo.BlobDeltaStepScript AS t
USING (
    SELECT
        CAST(2 AS tinyint)     AS StepNumber,
        N'MissingParentsBatch' AS ScriptKind,
        N'MissingParents'      AS StageName,
        CAST(1 AS bit)         AS UseParameterizedMaxDOP,
        N'Step 2: insert parents from queue (#Batch) into target.' AS Description
) AS s
ON t.StepNumber = s.StepNumber AND t.ScriptKind = s.ScriptKind
WHEN NOT MATCHED BY TARGET THEN
    INSERT (StepNumber, ScriptKind, StageName, ScriptBody, UseParameterizedMaxDOP, Description)
    VALUES (
        s.StepNumber,
        s.ScriptKind,
        s.StageName,
N'
INSERT INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],
     [is_archive],[is_system],[is_temporary])
SELECT
    RAFT.[stream_id],
    RAFT.[file_stream],
    RAFT.[name],
    RAFT.[path_locator],
    RAFT.[creation_time],
    RAFT.[last_write_time],
    RAFT.[last_access_time],
    RAFT.[is_directory],
    RAFT.[is_offline],
    RAFT.[is_hidden],
    RAFT.[is_readonly],
    RAFT.[is_archive],
    RAFT.[is_system],
    RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN #Batch B
    ON B.stream_id = RAFT.stream_id
OPTION (MAXDOP [MaxDOP]);
',
        s.UseParameterizedMaxDOP,
        s.Description
    )
WHEN MATCHED THEN
    UPDATE SET
        StageName           = s.StageName,
        ScriptBody          =
N'
INSERT INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],
     [is_archive],[is_system],[is_temporary])
SELECT
    RAFT.[stream_id],
    RAFT.[file_stream],
    RAFT.[name],
    RAFT.[path_locator],
    RAFT.[creation_time],
    RAFT.[last_write_time],
    RAFT.[last_access_time],
    RAFT.[is_directory],
    RAFT.[is_offline],
    RAFT.[is_hidden],
    RAFT.[is_readonly],
    RAFT.[is_archive],
    RAFT.[is_system],
    RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN #Batch B
    ON B.stream_id = RAFT.stream_id
OPTION (MAXDOP [MaxDOP]);
',
        UseParameterizedMaxDOP = s.UseParameterizedMaxDOP,
        Description            = s.Description;
GO

-- Step 3: Children (parent_path_locator IS NOT NULL), delta-windowed and BU-aware.
MERGE dbo.BlobDeltaStepScript AS t
USING (
    SELECT
        CAST(3 AS tinyint)     AS StepNumber,
        N'Children'            AS ScriptKind,
        N'Children'            AS StageName,
        CAST(0 AS bit)         AS UseParameterizedMaxDOP,
        N'Children: insert top batch where parent_path_locator IS NOT NULL, delta-windowed by metadata ModifiedOn.' AS Description
) AS s
ON t.StepNumber = s.StepNumber AND t.ScriptKind = s.ScriptKind
WHEN NOT MATCHED BY TARGET THEN
    INSERT (StepNumber, ScriptKind, StageName, ScriptBody, UseParameterizedMaxDOP, Description)
    VALUES (
        s.StepNumber,
        s.ScriptKind,
        s.StageName,
N'
INSERT TOP (@BatchSize) INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],
     [is_archive],[is_system],[is_temporary])
SELECT
    RAFT.[stream_id],
    RAFT.[file_stream],
    RAFT.[name],
    RAFT.[path_locator],
    RAFT.[creation_time],
    RAFT.[last_write_time],
    RAFT.[last_access_time],
    RAFT.[is_directory],
    RAFT.[is_offline],
    RAFT.[is_hidden],
    RAFT.[is_readonly],
    RAFT.[is_archive],
    RAFT.[is_system],
    RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN [MetadataTableFull] RAM WITH (NOLOCK)
    ON RAM.[MetadataIdColumn] = RAFT.stream_id
INNER JOIN [BusinessUnitTableFull] BU WITH (NOLOCK)
    ON BU.businessunit = RAM.OwningBusinessUnit
LEFT JOIN [TargetTableFull] TGT WITH (NOLOCK)
    ON TGT.stream_id = RAFT.stream_id
WHERE RAFT.parent_path_locator IS NOT NULL
  AND TGT.stream_id IS NULL
  AND RAFT.stream_id <> @ExcludedStreamId
  AND RAM.[MetadataModifiedOnColumn] >  @WindowStart
  AND RAM.[MetadataModifiedOnColumn] <= @WindowEnd
--  AND (CASE WHEN @BusinessUnitId IS NULL THEN 1 WHEN BU.businessunit = @BusinessUnitId THEN 1 ELSE 0 END = 1)
ORDER BY RAFT.stream_id
OPTION (MAXDOP 1);
',
        s.UseParameterizedMaxDOP,
        s.Description
    )
WHEN MATCHED THEN
    UPDATE SET
        StageName           = s.StageName,
        ScriptBody          =
N'
INSERT TOP (@BatchSize) INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],
     [is_archive],[is_system],[is_temporary])
SELECT
    RAFT.[stream_id],
    RAFT.[file_stream],
    RAFT.[name],
    RAFT.[path_locator],
    RAFT.[creation_time],
    RAFT.[last_write_time],
    RAFT.[last_access_time],
    RAFT.[is_directory],
    RAFT.[is_offline],
    RAFT.[is_hidden],
    RAFT.[is_readonly],
    RAFT.[is_archive],
    RAFT.[is_system],
    RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN [MetadataTableFull] RAM WITH (NOLOCK)
    ON RAM.[MetadataIdColumn] = RAFT.stream_id
INNER JOIN [BusinessUnitTableFull] BU WITH (NOLOCK)
    ON BU.businessunit = RAM.OwningBusinessUnit
LEFT JOIN [TargetTableFull] TGT WITH (NOLOCK)
    ON TGT.stream_id = RAFT.stream_id
WHERE RAFT.parent_path_locator IS NOT NULL
  AND TGT.stream_id IS NULL
  AND RAFT.stream_id <> @ExcludedStreamId
  AND RAM.[MetadataModifiedOnColumn] >  @WindowStart
  AND RAM.[MetadataModifiedOnColumn] <= @WindowEnd
--  AND (CASE WHEN @BusinessUnitId IS NULL THEN 1 WHEN BU.businessunit = @BusinessUnitId THEN 1 ELSE 0 END = 1)
ORDER BY RAFT.stream_id
OPTION (MAXDOP 1);
',
        UseParameterizedMaxDOP = s.UseParameterizedMaxDOP,
        Description            = s.Description;
GO

-- -----------------------------------------------------------------------------
-- 3. Queue population script (per-table, shared template for now)
-- -----------------------------------------------------------------------------

-- Queue PK is (RunId, TableName, stream_id). One parent can be referenced by children in
-- multiple business units, so we must deduplicate on stream_id only (one queue row per parent).
DECLARE @QueueScript nvarchar(max) = N'
INSERT INTO dbo.BlobDeltaMissingParentsQueue (RunId, TableName, stream_id, BusinessUnit, Processed, CreatedAt)
SELECT
    @RunId,
    @TableName,
    Par.stream_id,
    Par.businessunit,
    0,
    SYSDATETIME()
FROM (
    SELECT Par.stream_id, MIN(BU.businessunit) AS businessunit
    FROM [SourceTableFull] RAFT WITH (NOLOCK)
    INNER JOIN [MetadataTableFull] RAM WITH (NOLOCK)
        ON RAM.[MetadataIdColumn] = RAFT.stream_id
    INNER JOIN [BusinessUnitTableFull] BU WITH (NOLOCK)
        ON BU.businessunit = RAM.OwningBusinessUnit
    INNER JOIN [SourceTableFull] Par WITH (NOLOCK)
        ON Par.path_locator = RAFT.parent_path_locator
    WHERE RAFT.parent_path_locator IS NOT NULL
      AND RAFT.stream_id <> @ExcludedStreamId
      AND RAM.[MetadataModifiedOnColumn] >  @WindowStart
      AND RAM.[MetadataModifiedOnColumn] <= @WindowEnd
 --     AND (CASE WHEN @BusinessUnitId IS NULL THEN 1 WHEN BU.businessunit = @BusinessUnitId THEN 1 ELSE 0 END = 1)
    GROUP BY Par.stream_id
) Par
WHERE Par.stream_id <> @ExcludedStreamId
  AND NOT EXISTS (
      SELECT 1
      FROM [TargetTableFull] T WITH (NOLOCK)
      WHERE T.stream_id = Par.stream_id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM dbo.BlobDeltaMissingParentsQueue Q WITH (NOLOCK)
      WHERE Q.RunId = @RunId
        AND Q.TableName = @TableName
        AND Q.stream_id = Par.stream_id
  );
';

MERGE dbo.BlobDeltaQueuePopulationScript AS t
USING (
    SELECT TableName
    FROM dbo.BlobDeltaTableConfig
    WHERE IsActive = 1
      AND SourceDatabase = N'AdvancedRBSBlob_WCCIS'
) AS s
ON t.TableName = s.TableName
WHEN NOT MATCHED BY TARGET THEN
    INSERT (TableName, ScriptBody)
    VALUES (s.TableName, @QueueScript)
WHEN MATCHED THEN
    UPDATE SET ScriptBody = @QueueScript;
GO

PRINT N'BlobDeltaJobs config and script templates seeded/updated successfully.';
GO

-- -----------------------------------------------------------------------------
-- 4. Seed error handling policy (fatal vs non-fatal)
-- -----------------------------------------------------------------------------

;WITH PolicySeeds AS (
    SELECT
        CAST(207 AS int)          AS ErrorNumber,
        N'Invalid column name%'   AS ErrorMessagePattern,
        CAST(NULL AS tinyint)     AS AppliesToStep,
        CAST(NULL AS sysname)     AS AppliesToTableName,
        CAST(1 AS bit)            AS IsFatal,
        N'Table'                  AS ErrorScope,
        N'Invalid column name (schema/config issue) should fail the job.' AS Notes
    UNION ALL
    SELECT
        CAST(208 AS int),
        N'Invalid object name%',
        NULL,
        NULL,
        CAST(1 AS bit),
        N'Table',
        N'Invalid object name (schema/config issue) should fail the job.'
    UNION ALL
    -- Custom RAISERROR-based configuration issues from the engine (50000 by default)
    SELECT
        50000,
        N'TableName ''%'' not found or inactive in BlobDeltaTableConfig.%',
        NULL,
        NULL,
        CAST(1 AS bit),
        N'Table',
        N'Missing or inactive BlobDeltaTableConfig entry should fail the job.'
    UNION ALL
    SELECT
        50000,
        N'LA_BU table not found at % for table ''%''.%',
        NULL,
        NULL,
        CAST(1 AS bit),
        N'Table',
        N'Missing LA_BU table indicates bad configuration and should be fatal.'
    UNION ALL
    SELECT
        50000,
        N'Column ''businessunit'' not found in table % for table ''%''.%',
        NULL,
        NULL,
        CAST(1 AS bit),
        N'Table',
        N'Unexpected LA_BU schema; treat as fatal misconfiguration.'
    UNION ALL
    SELECT
        50000,
        N'Failed to parse TargetTableFull ''%'' for table ''%''.%',
        NULL,
        NULL,
        CAST(1 AS bit),
        N'Table',
        N'TargetTableFull parsing/configuration issue should be fatal.'
    UNION ALL
    SELECT
        50000,
        N'Script template not found for StepNumber=%, ScriptKind=''Roots''.%',
        CAST(1 AS tinyint),
        NULL,
        CAST(1 AS bit),
        N'Table',
        N'Missing Roots script template indicates broken deployment; fail fast.'
    UNION ALL
    SELECT
        50000,
        N'Script template not found for StepNumber=3, ScriptKind=''Children''.%',
        CAST(3 AS tinyint),
        NULL,
        CAST(1 AS bit),
        N'Table',
        N'Missing Children script template indicates broken deployment; fail fast.'
    UNION ALL
    SELECT
        50000,
        N'Queue population script not found for table ''%''.%',
        CAST(2 AS tinyint),
        NULL,
        CAST(1 AS bit),
        N'Table',
        N'Missing queue population script should be treated as fatal.'
)
MERGE dbo.BlobDeltaErrorPolicy AS t
USING PolicySeeds AS s
    ON  ISNULL(t.ErrorNumber, -1)                 = ISNULL(s.ErrorNumber, -1)
    AND ISNULL(t.ErrorMessagePattern, N'')        = ISNULL(s.ErrorMessagePattern, N'')
    AND ISNULL(CAST(t.AppliesToStep AS int), -1)  = ISNULL(CAST(s.AppliesToStep AS int), -1)
    AND ISNULL(t.AppliesToTableName, N'')         = ISNULL(s.AppliesToTableName, N'')
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        ErrorNumber,
        ErrorMessagePattern,
        AppliesToStep,
        AppliesToTableName,
        IsFatal,
        ErrorScope,
        Notes
    )
    VALUES (
        s.ErrorNumber,
        s.ErrorMessagePattern,
        s.AppliesToStep,
        s.AppliesToTableName,
        s.IsFatal,
        s.ErrorScope,
        s.Notes
    )
WHEN MATCHED THEN
    UPDATE SET
        t.IsFatal    = s.IsFatal,
        t.ErrorScope = s.ErrorScope,
        t.Notes      = s.Notes;
GO

-------------------------------------------------------------------------------
-- 5. Seed BlobDeltaTargetDatabases for @FileTableDatabase (insert-only)
-------------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.BlobDeltaTargetDatabases', N'U') IS NOT NULL
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM dbo.BlobDeltaTargetDatabases td
        WHERE td.TargetDatabase = @FileTableDatabase
    )
    BEGIN
        PRINT N'Seeding BlobDeltaTargetDatabases for ' + QUOTENAME(@FileTableDatabase) + N' with Extract = 1.';
        INSERT INTO dbo.BlobDeltaTargetDatabases (TargetDatabase, Extract)
        VALUES (@FileTableDatabase, 1);
    END
    ELSE
    BEGIN
        PRINT N'BlobDeltaTargetDatabases already has an entry for ' + QUOTENAME(@FileTableDatabase) + N'. Preserving existing Extract setting.';
    END
END
ELSE
BEGIN
    PRINT N'Warning: dbo.BlobDeltaTargetDatabases does not exist. Run the BlobDeltaJobs schema script to create it.';
END;
