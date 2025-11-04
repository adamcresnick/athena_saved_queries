CREATE OR REPLACE VIEW fhir_prd_db.v_radiation_summary AS
WITH

-- ============================================================================
-- CTE 1: Patients with ELECT structured data (observation-based)
-- ============================================================================
patients_with_structured_data AS (
    SELECT DISTINCT
        patient_fhir_id,
        COUNT(DISTINCT course_id) as num_structured_courses,
        MIN(obs_start_date) as earliest_structured_start,
        MAX(obs_stop_date) as latest_structured_end,
        SUM(CASE WHEN obs_dose_value IS NOT NULL THEN 1 ELSE 0 END) as num_dose_records,
        SUM(CASE WHEN obs_radiation_field IS NOT NULL THEN 1 ELSE 0 END) as num_site_records,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT obs_radiation_field), ', ') as radiation_fields_observed
    FROM fhir_prd_db.v_radiation_treatments
    WHERE data_source_primary = 'observation'
    GROUP BY patient_fhir_id
),

-- ============================================================================
-- CTE 2: Patients with radiation documents
-- ============================================================================
patients_with_documents AS (
    SELECT
        patient_fhir_id,
        COUNT(DISTINCT document_id) as num_radiation_documents,
        MIN(doc_date) as earliest_document_date,
        MAX(doc_date) as latest_document_date,
        -- Count by priority/type
        SUM(CASE WHEN extraction_priority = 1 THEN 1 ELSE 0 END) as num_treatment_summaries,
        SUM(CASE WHEN extraction_priority = 2 THEN 1 ELSE 0 END) as num_consults,
        SUM(CASE WHEN extraction_priority >= 3 THEN 1 ELSE 0 END) as num_other_documents,
        -- Document categories
        ARRAY_JOIN(ARRAY_SORT(ARRAY_AGG(DISTINCT document_category)), ', ') as document_types
    FROM fhir_prd_db.v_radiation_documents
    GROUP BY patient_fhir_id
),

-- ============================================================================
-- CTE 3: Patients with radiation care plans
-- ============================================================================
patients_with_care_plans AS (
    SELECT
        patient_fhir_id,
        COUNT(DISTINCT care_plan_id) as num_care_plans,
        MIN(cp_period_start) as earliest_care_plan_start,
        MAX(cp_period_end) as latest_care_plan_end,
        ARRAY_JOIN(ARRAY_SORT(ARRAY_AGG(DISTINCT cp_status)), ', ') as care_plan_statuses,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT SUBSTR(cp_title, 1, 50)), ' | ') as care_plan_titles_sample
    FROM fhir_prd_db.v_radiation_care_plan_hierarchy
    GROUP BY patient_fhir_id
),

-- ============================================================================
-- CTE 4: Patients with radiation appointments
-- ============================================================================
patients_with_appointments AS (
    SELECT
        patient_fhir_id,
        COUNT(DISTINCT appointment_id) as num_appointments,
        SUM(CASE WHEN appointment_status = 'fulfilled' THEN 1 ELSE 0 END) as num_fulfilled_appointments,
        SUM(CASE WHEN appointment_status = 'cancelled' THEN 1 ELSE 0 END) as num_cancelled_appointments,
        MIN(appointment_start) as earliest_appointment,
        MAX(appointment_start) as latest_appointment
    FROM fhir_prd_db.v_radiation_treatment_appointments
    GROUP BY patient_fhir_id
),

-- ============================================================================
-- CTE 5: Patients with service requests (treatment orders)
-- ============================================================================
patients_with_service_requests AS (
    SELECT DISTINCT
        patient_fhir_id,
        COUNT(DISTINCT course_id) as num_service_requests,
        MIN(sr_occurrence_period_start) as earliest_sr_start,
        MAX(sr_occurrence_period_end) as latest_sr_end,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT sr_code_text), ' | ') as service_request_codes
    FROM fhir_prd_db.v_radiation_treatments
    WHERE data_source_primary = 'service_request'
    GROUP BY patient_fhir_id
),

-- ============================================================================
-- CTE 6: Patients with ELECT radiation history flag (even without detailed data)
-- ============================================================================
patients_with_radiation_flag AS (
    SELECT DISTINCT subject_reference as patient_fhir_id
    FROM fhir_prd_db.observation
    WHERE code_text = 'ELECT - INTAKE FORM - TREATMENT HISTORY - RADIATION'
),

-- ============================================================================
-- CTE 7: Combine all patients who have ANY radiation data or radiation flag
-- ============================================================================
all_radiation_patients AS (
    SELECT DISTINCT patient_fhir_id FROM patients_with_structured_data
    UNION
    SELECT DISTINCT patient_fhir_id FROM patients_with_documents
    UNION
    SELECT DISTINCT patient_fhir_id FROM patients_with_care_plans
    UNION
    SELECT DISTINCT patient_fhir_id FROM patients_with_appointments
    UNION
    SELECT DISTINCT patient_fhir_id FROM patients_with_service_requests
    UNION
    SELECT DISTINCT patient_fhir_id FROM patients_with_radiation_flag
)

-- ============================================================================
-- MAIN SELECT: Data availability summary for each patient
-- ============================================================================
SELECT
    arp.patient_fhir_id as patient_fhir_id,

    -- ========================================================================
    -- DATA AVAILABILITY FLAGS (Boolean indicators)
    -- ========================================================================
    CASE WHEN prf.patient_fhir_id IS NOT NULL THEN true ELSE false END as has_radiation_history_flag,
    CASE WHEN psd.patient_fhir_id IS NOT NULL THEN true ELSE false END as has_structured_elect_data,
    CASE WHEN pd.patient_fhir_id IS NOT NULL THEN true ELSE false END as has_radiation_documents,
    CASE WHEN pcp.patient_fhir_id IS NOT NULL THEN true ELSE false END as has_care_plans,
    CASE WHEN pa.patient_fhir_id IS NOT NULL THEN true ELSE false END as has_appointments,
    CASE WHEN psr.patient_fhir_id IS NOT NULL THEN true ELSE false END as has_service_requests,

    -- ========================================================================
    -- DATA SOURCE COUNTS
    -- ========================================================================
    COALESCE(psd.num_structured_courses, 0) as num_structured_courses,
    COALESCE(pd.num_radiation_documents, 0) as num_radiation_documents,
    COALESCE(pcp.num_care_plans, 0) as num_care_plans,
    COALESCE(pa.num_appointments, 0) as num_appointments,
    COALESCE(psr.num_service_requests, 0) as num_service_requests,

    -- ========================================================================
    -- DOCUMENT BREAKDOWN (Priority-based counts)
    -- ========================================================================
    COALESCE(pd.num_treatment_summaries, 0) as num_treatment_summaries,
    COALESCE(pd.num_consults, 0) as num_consults,
    COALESCE(pd.num_other_documents, 0) as num_other_radiation_documents,

    -- ========================================================================
    -- APPOINTMENT BREAKDOWN
    -- ========================================================================
    COALESCE(pa.num_fulfilled_appointments, 0) as num_fulfilled_appointments,
    COALESCE(pa.num_cancelled_appointments, 0) as num_cancelled_appointments,

    -- ========================================================================
    -- TEMPORAL COVERAGE (Date ranges from each source)
    -- ========================================================================
    psd.earliest_structured_start as structured_data_earliest_date,
    psd.latest_structured_end as structured_data_latest_date,
    pd.earliest_document_date as documents_earliest_date,
    pd.latest_document_date as documents_latest_date,
    pcp.earliest_care_plan_start as care_plan_earliest_date,
    pcp.latest_care_plan_end as care_plan_latest_date,
    pa.earliest_appointment as appointments_earliest_date,
    pa.latest_appointment as appointments_latest_date,
    psr.earliest_sr_start as service_request_earliest_date,
    psr.latest_sr_end as service_request_latest_date,

    -- ========================================================================
    -- BEST AVAILABLE DATE RANGE (Across all sources)
    -- ========================================================================
    LEAST(
        psd.earliest_structured_start,
        pd.earliest_document_date,
        pcp.earliest_care_plan_start,
        pa.earliest_appointment,
        psr.earliest_sr_start
    ) as radiation_treatment_earliest_date,

    GREATEST(
        psd.latest_structured_end,
        pd.latest_document_date,
        pcp.latest_care_plan_end,
        pa.latest_appointment,
        psr.latest_sr_end
    ) as radiation_treatment_latest_date,

    -- ========================================================================
    -- STRUCTURED DATA DETAILS (When available)
    -- ========================================================================
    psd.num_dose_records,
    psd.num_site_records,
    psd.radiation_fields_observed,

    -- ========================================================================
    -- METADATA SAMPLES (For review/validation)
    -- ========================================================================
    pd.document_types as radiation_document_categories,
    pcp.care_plan_statuses,
    pcp.care_plan_titles_sample,
    psr.service_request_codes,

    -- ========================================================================
    -- DATA QUALITY / COMPLETENESS SCORE
    -- ========================================================================
    -- Score from 0-5 based on number of data sources available
    (
        CAST(CASE WHEN psd.patient_fhir_id IS NOT NULL THEN 1 ELSE 0 END AS INTEGER) +
        CAST(CASE WHEN pd.patient_fhir_id IS NOT NULL THEN 1 ELSE 0 END AS INTEGER) +
        CAST(CASE WHEN pcp.patient_fhir_id IS NOT NULL THEN 1 ELSE 0 END AS INTEGER) +
        CAST(CASE WHEN pa.patient_fhir_id IS NOT NULL THEN 1 ELSE 0 END AS INTEGER) +
        CAST(CASE WHEN psr.patient_fhir_id IS NOT NULL THEN 1 ELSE 0 END AS INTEGER)
    ) as num_data_sources_available,

    -- Normalized data quality score (0.0 to 1.0)
    CAST((
        CAST(CASE WHEN psd.patient_fhir_id IS NOT NULL THEN 1 ELSE 0 END AS DOUBLE) +
        CAST(CASE WHEN pd.patient_fhir_id IS NOT NULL THEN 1 ELSE 0 END AS DOUBLE) +
        CAST(CASE WHEN pcp.patient_fhir_id IS NOT NULL THEN 1 ELSE 0 END AS DOUBLE) +
        CAST(CASE WHEN pa.patient_fhir_id IS NOT NULL THEN 1 ELSE 0 END AS DOUBLE) +
        CAST(CASE WHEN psr.patient_fhir_id IS NOT NULL THEN 1 ELSE 0 END AS DOUBLE)
    ) / 5.0 AS DOUBLE) as data_completeness_score,

    -- ========================================================================
    -- RECOMMENDED EXTRACTION STRATEGY
    -- ========================================================================
    CASE
        -- Best case: Have structured data + documents
        WHEN psd.patient_fhir_id IS NOT NULL AND pd.patient_fhir_id IS NOT NULL
            THEN 'structured_primary_with_document_validation'

        -- Good case: Have treatment summaries/consults (priority 1-2 documents)
        WHEN pd.num_treatment_summaries > 0 OR pd.num_consults > 0
            THEN 'document_based_high_priority'

        -- Moderate case: Have documents but lower priority
        WHEN pd.patient_fhir_id IS NOT NULL
            THEN 'document_based_standard'

        -- Structured only (no documents for validation)
        WHEN psd.patient_fhir_id IS NOT NULL
            THEN 'structured_only_no_validation'

        -- Limited data: Only care plans or appointments
        WHEN pcp.patient_fhir_id IS NOT NULL OR pa.patient_fhir_id IS NOT NULL
            THEN 'metadata_only_limited_extraction'

        ELSE 'insufficient_data'
    END as recommended_extraction_strategy

FROM all_radiation_patients arp
LEFT JOIN patients_with_radiation_flag prf ON arp.patient_fhir_id = prf.patient_fhir_id
LEFT JOIN patients_with_structured_data psd ON arp.patient_fhir_id = psd.patient_fhir_id
LEFT JOIN patients_with_documents pd ON arp.patient_fhir_id = pd.patient_fhir_id
LEFT JOIN patients_with_care_plans pcp ON arp.patient_fhir_id = pcp.patient_fhir_id
LEFT JOIN patients_with_appointments pa ON arp.patient_fhir_id = pa.patient_fhir_id
LEFT JOIN patients_with_service_requests psr ON arp.patient_fhir_id = psr.patient_fhir_id

ORDER BY
    num_data_sources_available DESC,
    radiation_treatment_earliest_date;


-- ================================================================================

-- ================================================================================
-- VIEW: v_unified_patient_timeline
-- PURPOSE: Normalize ALL temporal events across FHIR domains
-- KEY UPDATES: Uses v_chemo_medications as source for accurate drug categorization
--              and therapeutic_normalized for drug continuity detection
-- ================================================================================

-- ================================================================================
-- UNIFIED PATIENT TIMELINE VIEW - COMPLETE VERSION
-- ================================================================================
-- Purpose: Normalize ALL temporal events across FHIR domains into single queryable view
-- Version: 2.3 (Streamlined to core clinical events)
-- Date: 2025-10-28
--
-- IMPORTANT: This view now works with datetime-standardized source views where:
--   - All VARCHAR datetime columns have been converted to TIMESTAMP(3)
--   - Date columns use DATE type
--   - Consistent typing enables simpler DATE() extraction instead of complex CAST logic
--
-- Coverage (8 core event sources):
--   - Diagnoses (v_diagnoses)
--   - Procedures (v_procedures_tumor)
--   - Imaging (v_imaging)
--   - Medications (v_chemo_medications)
--   - Visits (v_visits_unified) - unified encounters + appointments
--   - Measurements (v_measurements) - height, weight, vitals, labs
--   - Molecular Tests (v_molecular_tests)
--   - Radiation (v_radiation_treatment_appointments) - individual fractions
--
-- REMOVED (specialized/summary views moved out of core timeline):
--   - v_radiation_summary (patient-level summary, not event-level)
--   - v_ophthalmology_assessments (specialized assessments)
--   - v_audiology_assessments (specialized assessments)
--   - v_autologous_stem_cell_transplant (patient-level summary)
--   - v_autologous_stem_cell_collection (specialized procedure)
--   - v_imaging_corticosteroid_use (derived relationship, not primary event)
--
-- PROVENANCE TRACKING:
--   Every event includes:
--   - source_view: Which Athena view it came from (e.g., 'v_imaging', 'v_ophthalmology_assessments')
--   - source_domain: FHIR resource type (e.g., 'DiagnosticReport', 'Observation', 'Procedure')
--   - source_id: FHIR resource ID for traceability
--   - extraction_context: JSON with additional provenance metadata
--
-- USAGE:
--   -- Get all events for a patient
--   SELECT * FROM fhir_prd_db.v_unified_patient_timeline
--   WHERE patient_fhir_id = 'Patient/xyz'
--   ORDER BY event_date;
--
--   -- Get events in date range
--   SELECT * FROM fhir_prd_db.v_unified_patient_timeline
--   WHERE patient_fhir_id = 'Patient/xyz'
--     AND event_date BETWEEN '2018-01-01' AND '2019-12-31'
--   ORDER BY event_date;