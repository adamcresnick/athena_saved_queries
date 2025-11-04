CREATE OR REPLACE VIEW fhir_prd_db.v_imaging AS
WITH report_categories AS (
    SELECT
        diagnostic_report_id,
        LISTAGG(DISTINCT category_text, ' | ') WITHIN GROUP (ORDER BY category_text) as category_text
    FROM fhir_prd_db.diagnostic_report_category
    GROUP BY diagnostic_report_id
),
mri_imaging AS (
    -- Aggregate MRI results to prevent JOIN explosion (one row per MRI study)
    SELECT
        mri.patient_id,
        mri.imaging_procedure_id,
        TRY(CAST(FROM_ISO8601_TIMESTAMP(NULLIF(mri.result_datetime, '')) AS TIMESTAMP(3))) as imaging_date,
        mri.imaging_procedure,
        mri.result_diagnostic_report_id,
        'MRI' as imaging_modality,
        -- Aggregate multiple result components into single field
        LISTAGG(DISTINCT results.value_string, ' | ') WITHIN GROUP (ORDER BY results.value_string) as result_information,
        LISTAGG(DISTINCT results.result_display, ' | ') WITHIN GROUP (ORDER BY results.result_display) as result_display
    FROM fhir_prd_db.radiology_imaging_mri mri
    LEFT JOIN fhir_prd_db.radiology_imaging_mri_results results
        ON mri.imaging_procedure_id = results.imaging_procedure_id
    GROUP BY mri.patient_id, mri.imaging_procedure_id, mri.result_datetime,
             mri.imaging_procedure, mri.result_diagnostic_report_id
),
other_imaging AS (
    -- Exclude MRIs that are already in mri_imaging to prevent duplicates from UNION
    SELECT
        ri.patient_id,
        ri.imaging_procedure_id,
        TRY(CAST(FROM_ISO8601_TIMESTAMP(NULLIF(ri.result_datetime, '')) AS TIMESTAMP(3))) as imaging_date,
        ri.imaging_procedure,
        ri.result_diagnostic_report_id,
        COALESCE(ri.imaging_procedure, 'Unknown') as imaging_modality,
        CAST(NULL AS VARCHAR) as result_information,
        CAST(NULL AS VARCHAR) as result_display
    FROM fhir_prd_db.radiology_imaging ri
    LEFT JOIN fhir_prd_db.radiology_imaging_mri mri
        ON ri.imaging_procedure_id = mri.imaging_procedure_id
    WHERE mri.imaging_procedure_id IS NULL  -- Exclude MRIs already in mri_imaging
),
combined_imaging AS (
    SELECT * FROM mri_imaging
    UNION ALL
    SELECT * FROM other_imaging
)
SELECT
    ci.patient_id as patient_fhir_id,
    ci.patient_id as patient_mrn,  -- Using FHIR ID
    ci.imaging_procedure_id,
    ci.imaging_date,
    ci.imaging_procedure,
    ci.result_diagnostic_report_id,
    ci.imaging_modality,
    ci.result_information,
    ci.result_display,

    -- Diagnostic report fields
    dr.id as diagnostic_report_id,
    dr.status as report_status,
    dr.conclusion as report_conclusion,
    TRY(CAST(dr.issued AS TIMESTAMP(3))) as report_issued,
    TRY(CAST(dr.effective_period_start AS TIMESTAMP(3))) as report_effective_period_start,
    TRY(CAST(dr.effective_period_stop AS TIMESTAMP(3))) as report_effective_period_stop,
    rc.category_text,

    -- Age calculations
    TRY(DATE_DIFF('day',
        DATE(pa.birth_date),
        DATE(ci.imaging_date))) as age_at_imaging_days,
    TRY(DATE_DIFF('year',
        DATE(pa.birth_date),
        DATE(ci.imaging_date))) as age_at_imaging_years,

    -- V4.1 Enhancements
    dp.performer_reference AS performer_org_id,
    org.name AS performer_org_name,
    drc.content_attachment_url AS binary_content_id

FROM combined_imaging ci
LEFT JOIN fhir_prd_db.diagnostic_report dr
    ON ci.result_diagnostic_report_id = dr.id
LEFT JOIN report_categories rc
    ON dr.id = rc.diagnostic_report_id
LEFT JOIN fhir_prd_db.patient_access pa
    ON ci.patient_id = pa.id
LEFT JOIN
    fhir_prd_db.diagnostic_report_performer dp ON dr.id = dp.diagnostic_report_id
LEFT JOIN
    fhir_prd_db.organization org ON dp.performer_reference = org.id
LEFT JOIN
    fhir_prd_db.document_reference_content drc ON dr.id = drc.document_reference_id
WHERE ci.patient_id IS NOT NULL
ORDER BY ci.patient_id, ci.imaging_date DESC;
