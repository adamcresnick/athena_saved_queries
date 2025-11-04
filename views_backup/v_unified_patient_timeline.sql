CREATE OR REPLACE VIEW fhir_prd_db.v_unified_patient_timeline AS

-- ============================================================================
-- 1. DIAGNOSES AS EVENTS
-- Source: v_diagnoses (includes problem_list_diagnoses, hydrocephalus_diagnosis)
-- Provenance: Condition FHIR resource
-- ============================================================================
SELECT
    vd.patient_fhir_id,
    'diag_' || vd.condition_id as event_id,
    DATE(vd.onset_date_time) as event_date,  -- onset_date_time is TIMESTAMP(3) in v_diagnoses
    vd.age_at_onset_days as age_at_event_days,
    CAST(vd.age_at_onset_days AS DOUBLE) / 365.25 as age_at_event_years,

    -- Event classification
    'Diagnosis' as event_type,
    CASE
        WHEN vd.diagnosis_name LIKE '%neoplasm%' OR vd.diagnosis_name LIKE '%tumor%'
             OR vd.diagnosis_name LIKE '%astrocytoma%' OR vd.diagnosis_name LIKE '%glioma%'
             OR vd.diagnosis_name LIKE '%medulloblastoma%' OR vd.diagnosis_name LIKE '%ependymoma%'
        THEN 'Tumor'
        WHEN vd.diagnosis_name LIKE '%chemotherapy%' OR vd.diagnosis_name LIKE '%nausea%'
             OR vd.diagnosis_name LIKE '%vomiting%' OR vd.diagnosis_name LIKE '%induced%'
        THEN 'Treatment Toxicity'
        WHEN vd.diagnosis_name LIKE '%hydrocephalus%' THEN 'Hydrocephalus'
        WHEN vd.diagnosis_name LIKE '%vision%' OR vd.diagnosis_name LIKE '%visual%'
             OR vd.diagnosis_name LIKE '%diplopia%' OR vd.diagnosis_name LIKE '%nystagmus%'
        THEN 'Vision Disorder'
        WHEN vd.diagnosis_name LIKE '%hearing%' OR vd.diagnosis_name LIKE '%ototoxic%'
        THEN 'Hearing Disorder'
        ELSE 'Other Complication'
    END as event_category,
    CASE
        WHEN vd.diagnosis_name LIKE '%progression%' OR vd.snomed_code = 25173007 THEN 'Progression'
        WHEN vd.diagnosis_name LIKE '%recurrence%' OR vd.diagnosis_name LIKE '%recurrent%' THEN 'Recurrence'
        WHEN vd.diagnosis_name LIKE '%astrocytoma%' OR vd.diagnosis_name LIKE '%glioma%'
             OR vd.diagnosis_name LIKE '%medulloblastoma%' THEN 'Initial Diagnosis'
        ELSE NULL
    END as event_subtype,
    vd.diagnosis_name as event_description,
    vd.clinical_status_text as event_status,

    -- Provenance
    'v_diagnoses' as source_view,
    'Condition' as source_domain,
    vd.condition_id as source_id,

    -- Clinical codes
    ARRAY[vd.icd10_code] as icd10_codes,
    ARRAY[CAST(vd.snomed_code AS VARCHAR)] as snomed_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as cpt_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as loinc_codes,

    -- Event metadata (domain-specific fields)
    CAST(CAST(MAP(
        ARRAY['icd10_code', 'icd10_display', 'snomed_code', 'snomed_display', 'recorded_date', 'abatement_date_time', 'clinical_status'],
        ARRAY[
            CAST(vd.icd10_code AS VARCHAR),
            CAST(vd.icd10_display AS VARCHAR),
            CAST(vd.snomed_code AS VARCHAR),
            CAST(vd.snomed_display AS VARCHAR),
            CAST(vd.recorded_date AS VARCHAR),
            CAST(vd.abatement_date_time AS VARCHAR),
            CAST(vd.clinical_status_text AS VARCHAR)
        ]
    ) AS JSON) AS VARCHAR) as event_metadata,

    -- Extraction context (for agent queries)
    CAST(CAST(MAP(
        ARRAY['source_view', 'source_table', 'extraction_timestamp', 'has_structured_code', 'requires_free_text_extraction'],
        ARRAY[
            'v_diagnoses',
            'condition',
            CAST(CURRENT_TIMESTAMP AS VARCHAR),
            CAST(CASE WHEN vd.icd10_code IS NOT NULL OR vd.snomed_code IS NOT NULL THEN true ELSE false END AS VARCHAR),
            'false'
        ]
    ) AS JSON) AS VARCHAR) as extraction_context

FROM fhir_prd_db.v_diagnoses vd

UNION ALL

-- ============================================================================
-- 2. PROCEDURES AS EVENTS
-- Source: v_procedures_tumor (tumor-related procedures only)
-- Provenance: Procedure FHIR resource
-- ============================================================================
SELECT
    vp.patient_fhir_id,
    'proc_' || vp.procedure_fhir_id as event_id,
    CAST(vp.procedure_date AS DATE) as event_date,
    vp.age_at_procedure_days as age_at_event_days,
    CAST(vp.age_at_procedure_days AS DOUBLE) / 365.25 as age_at_event_years,

    'Procedure' as event_type,
    CASE
        WHEN vp.cpt_classification IN ('craniotomy_tumor_resection', 'stereotactic_tumor_procedure', 'neuroendoscopy_tumor', 'open_brain_biopsy', 'skull_base_tumor') THEN 'Tumor Surgery'
        WHEN vp.cpt_classification IN ('tumor_related_csf_management', 'tumor_related_device_implant') THEN 'Supportive Procedure'
        WHEN vp.surgery_type IN ('biopsy', 'stereotactic_procedure') THEN 'Biopsy/Diagnostic'
        WHEN vp.surgery_type IN ('craniotomy', 'craniectomy', 'neuroendoscopy', 'skull_base') THEN 'Tumor Surgery'
        ELSE 'Other Procedure'
    END as event_category,
    COALESCE(vp.cpt_classification, vp.procedure_classification, vp.surgery_type) as event_subtype,
    vp.proc_code_text as event_description,
    vp.proc_status as event_status,

    'v_procedures_tumor' as source_view,
    'Procedure' as source_domain,
    vp.procedure_fhir_id as source_id,

    CAST(NULL AS ARRAY(VARCHAR)) as icd10_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as snomed_codes,
    ARRAY[vp.cpt_code] as cpt_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as loinc_codes,

    CAST(CAST(MAP(
        ARRAY['procedure_type', 'cpt_code', 'cpt_classification', 'surgery_type', 'body_site', 'performed_by', 'procedure_status', 'classification_confidence', 'is_tumor_surgery'],
        ARRAY[
            CAST(vp.proc_code_text AS VARCHAR),
            CAST(vp.cpt_code AS VARCHAR),
            CAST(vp.cpt_classification AS VARCHAR),
            CAST(vp.surgery_type AS VARCHAR),
            CAST(vp.pbs_body_site_text AS VARCHAR),
            CAST(vp.pp_performer_actor_display AS VARCHAR),
            CAST(vp.proc_status AS VARCHAR),
            CAST(vp.classification_confidence AS VARCHAR),
            CAST(vp.is_tumor_surgery AS VARCHAR)
        ]
    ) AS JSON) AS VARCHAR) as event_metadata,

    CAST(CAST(MAP(
        ARRAY['source_view', 'source_table', 'extraction_timestamp', 'has_structured_code', 'requires_free_text_extraction'],
        ARRAY[
            'v_procedures_tumor',
            'procedure',
            CAST(CURRENT_TIMESTAMP AS VARCHAR),
            CAST(CASE WHEN vp.cpt_code IS NOT NULL THEN true ELSE false END AS VARCHAR),
            'false'
        ]
    ) AS JSON) AS VARCHAR) as extraction_context

FROM fhir_prd_db.v_procedures_tumor vp
WHERE vp.is_tumor_surgery = true

UNION ALL

-- ============================================================================
-- 3. IMAGING AS EVENTS
-- Source: v_imaging
-- Provenance: DiagnosticReport FHIR resource
-- ============================================================================
SELECT
    vi.patient_fhir_id,
    'img_' || vi.imaging_procedure_id as event_id,
    CAST(vi.imaging_date AS DATE) as event_date,
    vi.age_at_imaging_days as age_at_event_days,
    CAST(vi.age_at_imaging_days AS DOUBLE) / 365.25 as age_at_event_years,

    'Imaging' as event_type,
    'Imaging' as event_category,
    CASE
        WHEN LOWER(vi.report_conclusion) LIKE '%progression%' OR LOWER(vi.report_conclusion) LIKE '%increase%'
        THEN 'Progression Imaging'
        WHEN LOWER(vi.report_conclusion) LIKE '%stable%' THEN 'Stable Imaging'
        WHEN LOWER(vi.report_conclusion) LIKE '%improvement%' OR LOWER(vi.report_conclusion) LIKE '%decrease%'
        THEN 'Response Imaging'
        ELSE 'Surveillance Imaging'
    END as event_subtype,
    vi.imaging_procedure as event_description,
    vi.report_status as event_status,

    'v_imaging' as source_view,
    'DiagnosticReport' as source_domain,
    vi.imaging_procedure_id as source_id,

    CAST(NULL AS ARRAY(VARCHAR)) as icd10_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as snomed_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as cpt_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as loinc_codes,

    CAST(CAST(MAP(
        ARRAY['modality', 'category', 'report_conclusion', 'report_status', 'report_issued', 'result_display'],
        ARRAY[
            CAST(vi.imaging_modality AS VARCHAR),
            CAST(vi.category_text AS VARCHAR),
            CAST(vi.report_conclusion AS VARCHAR),
            CAST(vi.report_status AS VARCHAR),
            CAST(vi.report_issued AS VARCHAR),
            CAST(vi.result_display AS VARCHAR)
        ]
    ) AS JSON) AS VARCHAR) as event_metadata,

    CAST(CAST(MAP(
        ARRAY['source_view', 'source_table', 'extraction_timestamp', 'has_structured_code', 'requires_free_text_extraction', 'free_text_fields'],
        ARRAY[
            'v_imaging',
            'diagnostic_report',
            CAST(CURRENT_TIMESTAMP AS VARCHAR),
            'false',
            CAST(CASE WHEN vi.report_conclusion IS NOT NULL THEN true ELSE false END AS VARCHAR),
            'report_conclusion, result_display'
        ]
    ) AS JSON) AS VARCHAR) as extraction_context

FROM fhir_prd_db.v_imaging vi

UNION ALL

-- ============================================================================
-- 4. MEDICATIONS AS EVENTS (Chemotherapy & Systemic Therapy)
-- Source: v_chemo_medications (comprehensive chemotherapy filtering with drug categorization)
-- Provenance: MedicationRequest FHIR resource
-- NOTE: Using v_chemo_medications instead of v_medications for accurate drug categorization
--       and to include therapeutic_normalized for drug continuity detection
-- ============================================================================
SELECT
    vcm.patient_fhir_id,
    'med_' || vcm.medication_request_fhir_id as event_id,
    -- FIXED: Parse ISO8601 timestamp string before casting to DATE
    -- medication_start_date from v_chemo_medications is VARCHAR containing ISO8601 timestamps like "2019-09-17T15:41:00Z"
    CAST(TRY(FROM_ISO8601_TIMESTAMP(vcm.medication_start_date)) AS DATE) as event_date,
    DATE_DIFF('day', CAST(vpd.pd_birth_date AS DATE), CAST(TRY(FROM_ISO8601_TIMESTAMP(vcm.medication_start_date)) AS DATE)) as age_at_event_days,
    CAST(DATE_DIFF('day', CAST(vpd.pd_birth_date AS DATE), CAST(TRY(FROM_ISO8601_TIMESTAMP(vcm.medication_start_date)) AS DATE)) AS DOUBLE) / 365.25 as age_at_event_years,

    'Medication' as event_type,
    -- Use accurate drug_category from v_chemo_medications instead of LIKE pattern matching
    COALESCE(vcm.chemo_drug_category, 'Other Medication') as event_category,
    vcm.chemo_preferred_name as event_subtype,
    vcm.medication_name as event_description,
    vcm.medication_status as event_status,

    'v_chemo_medications' as source_view,
    'MedicationRequest' as source_domain,
    vcm.medication_request_fhir_id as source_id,

    CAST(NULL AS ARRAY(VARCHAR)) as icd10_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as snomed_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as cpt_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as loinc_codes,

    CAST(CAST(MAP(
        ARRAY['medication_name', 'chemo_preferred_name', 'chemo_therapeutic_normalized', 'chemo_drug_category', 'rx_norm_codes', 'start_date', 'stop_date', 'status', 'route', 'dosage'],
        ARRAY[
            CAST(vcm.medication_name AS VARCHAR),
            CAST(vcm.chemo_preferred_name AS VARCHAR),
            CAST(vcm.chemo_therapeutic_normalized AS VARCHAR),
            CAST(vcm.chemo_drug_category AS VARCHAR),
            CAST(vcm.medication_rxnorm_code AS VARCHAR),
            CAST(vcm.medication_start_date AS VARCHAR),
            CAST(vcm.medication_stop_date AS VARCHAR),
            CAST(vcm.medication_status AS VARCHAR),
            CAST(vcm.medication_route AS VARCHAR),
            CAST(vcm.medication_dosage_instructions AS VARCHAR)
        ]
    ) AS JSON) AS VARCHAR) as event_metadata,

    CAST(CAST(MAP(
        ARRAY['source_view', 'source_table', 'extraction_timestamp', 'has_structured_code', 'match_type', 'therapeutic_normalized'],
        ARRAY[
            'v_chemo_medications',
            'medication_request',
            CAST(CURRENT_TIMESTAMP AS VARCHAR),
            CAST(CASE WHEN vcm.medication_rxnorm_code IS NOT NULL THEN true ELSE false END AS VARCHAR),
            CAST(vcm.rxnorm_match_type AS VARCHAR),
            CAST(vcm.chemo_therapeutic_normalized AS VARCHAR)
        ]
    ) AS JSON) AS VARCHAR) as extraction_context

FROM fhir_prd_db.v_chemo_medications vcm
LEFT JOIN fhir_prd_db.v_patient_demographics vpd ON vcm.patient_fhir_id = vpd.patient_fhir_id

UNION ALL

-- ============================================================================
-- 5. VISITS (ENCOUNTERS + APPOINTMENTS) AS EVENTS
-- Source: v_visits_unified
-- Provenance: Encounter + Appointment FHIR resources (unified)
-- ============================================================================
SELECT
    vv.patient_fhir_id,
    'visit_' || COALESCE(vv.encounter_id, vv.appointment_fhir_id) as event_id,
    vv.visit_date as event_date,
    vv.age_at_visit_days as age_at_event_days,
    CAST(vv.age_at_visit_days AS DOUBLE) / 365.25 as age_at_event_years,

    'Visit' as event_type,
    CASE
        WHEN vv.visit_type = 'completed_scheduled' THEN 'Completed Visit'
        WHEN vv.visit_type = 'completed_no_encounter' THEN 'Appointment Only'
        WHEN vv.visit_type = 'no_show' THEN 'No Show'
        WHEN vv.visit_type = 'cancelled' THEN 'Cancelled Visit'
        WHEN vv.visit_type = 'future_scheduled' THEN 'Scheduled Visit'
        WHEN vv.source = 'encounter_only' THEN 'Unscheduled Encounter'
        ELSE 'Other Visit'
    END as event_category,
    COALESCE(vv.appointment_type_text, vv.encounter_status) as event_subtype,
    COALESCE(
        vv.appointment_description,
        vv.appointment_type_text,
        'Visit on ' || CAST(vv.visit_date AS VARCHAR)
    ) as event_description,
    COALESCE(vv.appointment_status, vv.encounter_status) as event_status,

    'v_visits_unified' as source_view,
    'Encounter+Appointment' as source_domain,
    COALESCE(vv.encounter_id, vv.appointment_fhir_id) as source_id,

    CAST(NULL AS ARRAY(VARCHAR)) as icd10_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as snomed_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as cpt_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as loinc_codes,

    CAST(CAST(MAP(
        ARRAY['visit_type', 'appointment_status', 'appointment_type', 'encounter_status', 'appointment_start', 'appointment_end', 'encounter_start', 'encounter_end', 'appointment_completed', 'encounter_occurred'],
        ARRAY[
            CAST(vv.visit_type AS VARCHAR),
            CAST(vv.appointment_status AS VARCHAR),
            CAST(vv.appointment_type_text AS VARCHAR),
            CAST(vv.encounter_status AS VARCHAR),
            CAST(vv.appointment_start AS VARCHAR),
            CAST(vv.appointment_end AS VARCHAR),
            CAST(vv.encounter_start AS VARCHAR),
            CAST(vv.encounter_end AS VARCHAR),
            CAST(vv.appointment_completed AS VARCHAR),
            CAST(vv.encounter_occurred AS VARCHAR)
        ]
    ) AS JSON) AS VARCHAR) as event_metadata,

    CAST(CAST(MAP(
        ARRAY['source_view', 'source_table', 'extraction_timestamp', 'has_structured_code', 'requires_free_text_extraction'],
        ARRAY[
            'v_visits_unified',
            'encounter + appointment',
            CAST(CURRENT_TIMESTAMP AS VARCHAR),
            'false',
            'false'
        ]
    ) AS JSON) AS VARCHAR) as extraction_context

FROM fhir_prd_db.v_visits_unified vv

UNION ALL

-- ============================================================================
-- 6. MOLECULAR TESTS AS EVENTS
-- Source: v_molecular_tests
-- Provenance: Observation (Lab) FHIR resource
-- ============================================================================
SELECT
    vmt.patient_fhir_id,
    'moltest_' || vmt.mt_test_id as event_id,
    CAST(vmt.mt_test_date AS DATE) as event_date,
    vmt.age_at_test_days as age_at_event_days,
    CAST(vmt.age_at_test_days AS DOUBLE) / 365.25 as age_at_event_years,

    'Molecular Test' as event_type,
    'Genomic Testing' as event_category,
    vmt.mt_lab_test_name as event_subtype,
    vmt.mt_lab_test_name as event_description,
    vmt.mt_test_status as event_status,

    'v_molecular_tests' as source_view,
    'Observation' as source_domain,
    vmt.mt_test_id as source_id,

    CAST(NULL AS ARRAY(VARCHAR)) as icd10_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as snomed_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as cpt_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as loinc_codes,

    CAST(CAST(MAP(
        ARRAY['test_name', 'specimen_types', 'specimen_sites', 'specimen_ids', 'specimen_collection_date', 'component_count', 'narrative_chars', 'requester'],
        ARRAY[
            CAST(vmt.mt_lab_test_name AS VARCHAR),
            CAST(vmt.mt_specimen_types AS VARCHAR),
            CAST(vmt.mt_specimen_sites AS VARCHAR),
            CAST(vmt.mt_specimen_ids AS VARCHAR),
            CAST(vmt.mt_specimen_collection_date AS VARCHAR),
            CAST(vmt.mtr_component_count AS VARCHAR),
            CAST(vmt.mtr_total_narrative_chars AS VARCHAR),
            CAST(vmt.mt_test_requester AS VARCHAR)
        ]
    ) AS JSON) AS VARCHAR) as event_metadata,

    CAST(CAST(MAP(
        ARRAY['source_view', 'source_table', 'extraction_timestamp', 'has_structured_code', 'requires_free_text_extraction', 'free_text_fields'],
        ARRAY[
            'v_molecular_tests',
            'lab_tests + lab_test_results',
            CAST(CURRENT_TIMESTAMP AS VARCHAR),
            'false',
            CAST(CASE WHEN vmt.mtr_total_narrative_chars > 0 THEN true ELSE false END AS VARCHAR),
            'test_result_narrative'
        ]
    ) AS JSON) AS VARCHAR) as extraction_context

FROM fhir_prd_db.v_molecular_tests vmt

UNION ALL

-- ============================================================================
-- 7A. RADIATION TREATMENT COURSES AS EVENTS
-- Source: v_radiation_summary (aggregated course data)
-- Provenance: CarePlan FHIR resource

-- ============================================================================
-- 7B. RADIATION TREATMENT APPOINTMENTS AS EVENTS (Individual Fractions)
-- Source: v_radiation_treatment_appointments
-- Provenance: Appointment FHIR resource
-- MODERATE PRIORITY - Adds granular daily treatment data
-- ============================================================================
SELECT
    vrta.patient_fhir_id,
    'radfx_' || vrta.appointment_id as event_id,
    DATE(vrta.appointment_start) as event_date,  -- appointment_start is TIMESTAMP(3) after standardization
    DATE_DIFF('day', DATE(vpd.pd_birth_date), DATE(vrta.appointment_start)) as age_at_event_days,
    CAST(DATE_DIFF('day', DATE(vpd.pd_birth_date), DATE(vrta.appointment_start)) AS DOUBLE) / 365.25 as age_at_event_years,

    'Radiation Fraction' as event_type,
    'Radiation Therapy' as event_category,
    'Daily Treatment' as event_subtype,
    'Radiation treatment fraction' as event_description,
    vrta.appointment_status as event_status,

    'v_radiation_treatment_appointments' as source_view,
    'Appointment' as source_domain,
    vrta.appointment_id as source_id,

    CAST(NULL AS ARRAY(VARCHAR)) as icd10_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as snomed_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as cpt_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as loinc_codes,

    CAST(CAST(MAP(
        ARRAY['appointment_start', 'appointment_end', 'appointment_type', 'appointment_status', 'minutes_duration'],
        ARRAY[
            CAST(vrta.appointment_start AS VARCHAR),
            CAST(vrta.appointment_end AS VARCHAR),
            CAST(vrta.appointment_type_text AS VARCHAR),
            CAST(vrta.appointment_status AS VARCHAR),
            CAST(vrta.minutes_duration AS VARCHAR)
        ]
    ) AS JSON) AS VARCHAR) as event_metadata,

    CAST(CAST(MAP(
        ARRAY['source_view', 'source_table', 'extraction_timestamp', 'has_structured_code', 'requires_free_text_extraction'],
        ARRAY[
            'v_radiation_treatment_appointments',
            'appointment',
            CAST(CURRENT_TIMESTAMP AS VARCHAR),
            'false',
            'false'
        ]
    ) AS JSON) AS VARCHAR) as extraction_context

FROM fhir_prd_db.v_radiation_treatment_appointments vrta
LEFT JOIN fhir_prd_db.v_patient_demographics vpd ON vrta.patient_fhir_id = vpd.patient_fhir_id

UNION ALL

-- ============================================================================
-- 8. MEASUREMENTS AS EVENTS (Growth, Vitals, Labs)
-- Source: v_measurements
-- Provenance: Observation + LabTests FHIR resources
-- CRITICAL PRIORITY - 1,570 records for patient e4BwD8ZYDBccepXcJ.Ilo3w3
-- ============================================================================
SELECT
    vm.patient_fhir_id,
    COALESCE('obs_' || vm.obs_observation_id, 'lab_' || vm.lt_test_id) as event_id,
    CAST(COALESCE(vm.obs_measurement_date, vm.lt_measurement_date) AS DATE) as event_date,
    vm.age_at_measurement_days as age_at_event_days,
    vm.age_at_measurement_years as age_at_event_years,

    'Measurement' as event_type,
    CASE
        WHEN LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) IN ('height', 'weight', 'bmi', 'body mass index')
        THEN 'Growth'
        WHEN LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%blood pressure%'
             OR LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%heart rate%'
             OR LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%temperature%'
             OR LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%oxygen%'
        THEN 'Vital Signs'
        WHEN LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%cbc%'
             OR LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%blood count%'
             OR LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%hemoglobin%'
             OR LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%platelet%'
             OR LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%wbc%'
             OR LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%neutrophil%'
        THEN 'Hematology Lab'
        WHEN LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%metabolic%'
             OR LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%chemistry%'
             OR LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%electrolyte%'
        THEN 'Chemistry Lab'
        WHEN LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%liver%'
             OR LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%alt%'
             OR LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%ast%'
             OR LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%bilirubin%'
        THEN 'Liver Function'
        WHEN LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%renal%'
             OR LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%creatinine%'
             OR LOWER(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type)) LIKE '%bun%'
        THEN 'Renal Function'
        ELSE 'Other Lab'
    END as event_category,
    COALESCE(vm.obs_measurement_type, vm.lt_measurement_type) as event_subtype,
    COALESCE(vm.obs_measurement_type, vm.lt_measurement_type) as event_description,
    COALESCE(vm.obs_status, vm.lt_status) as event_status,

    'v_measurements' as source_view,
    vm.source_table as source_domain,  -- 'observation' or 'lab_tests'
    COALESCE(vm.obs_observation_id, vm.lt_test_id) as source_id,

    CAST(NULL AS ARRAY(VARCHAR)) as icd10_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as snomed_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as cpt_codes,
    CAST(NULL AS ARRAY(VARCHAR)) as loinc_codes,

    CAST(CAST(MAP(
        ARRAY['measurement_type', 'measurement_value', 'measurement_unit', 'value_range_low', 'value_range_high', 'test_component', 'source_table'],
        ARRAY[
            CAST(COALESCE(vm.obs_measurement_type, vm.lt_measurement_type) AS VARCHAR),
            COALESCE(CAST(vm.obs_measurement_value AS VARCHAR), vm.ltr_value_string),
            CAST(COALESCE(vm.obs_measurement_unit, vm.ltr_measurement_unit) AS VARCHAR),
            CAST(vm.ltr_value_range_low_value AS VARCHAR),
            CAST(vm.ltr_value_range_high_value AS VARCHAR),
            CAST(vm.ltr_test_component AS VARCHAR),
            CAST(vm.source_table AS VARCHAR)
        ]
    ) AS JSON) AS VARCHAR) as event_metadata,

    CAST(CAST(MAP(
        ARRAY['source_view', 'source_table', 'extraction_timestamp', 'has_structured_code', 'requires_free_text_extraction', 'measurement_is_quantitative'],
        ARRAY[
            'v_measurements',
            CAST(vm.source_table AS VARCHAR),
            CAST(CURRENT_TIMESTAMP AS VARCHAR),
            'false',
            'false',
            CAST(CASE WHEN vm.obs_measurement_value IS NOT NULL OR vm.ltr_value_range_low_value IS NOT NULL THEN true ELSE false END AS VARCHAR)
        ]
    ) AS JSON) AS VARCHAR) as extraction_context

FROM fhir_prd_db.v_measurements vm

UNION ALL

-- ============================================================================
-- 9. OPHTHALMOLOGY ASSESSMENTS AS EVENTS
-- Source: v_ophthalmology_assessments
-- Provenance: Observation + Procedure + DocumentReference FHIR resources
-- CRITICAL PRIORITY - Visual acuity, visual fields, OCT, fundus exams

-- ============================================================================
-- 10. AUDIOLOGY ASSESSMENTS AS EVENTS
-- Source: v_audiology_assessments
-- Provenance: Observation + Procedure + Condition FHIR resources
-- CRITICAL PRIORITY - Audiograms, ototoxicity monitoring

-- ============================================================================
-- 11. AUTOLOGOUS STEM CELL TRANSPLANT AS EVENTS
-- Source: v_autologous_stem_cell_transplant
-- Provenance: Condition + Procedure FHIR resources
-- HIGH PRIORITY - Major treatment modality

-- ============================================================================
-- 12. AUTOLOGOUS STEM CELL COLLECTION AS EVENTS
-- Source: v_autologous_stem_cell_collection
-- Provenance: Procedure FHIR resource
-- HIGH PRIORITY - Preparatory procedure for transplant

-- ============================================================================
-- 13. IMAGING-RELATED CORTICOSTEROID USE AS EVENTS
-- Source: v_imaging_corticosteroid_use
-- Provenance: MedicationRequest FHIR resource (linked to imaging)
-- HIGH PRIORITY - Steroid timing signals tumor edema and affects imaging interpretation
ORDER BY patient_fhir_id, event_date, event_type;


-- ================================================================================
-- VALIDATION QUERIES
-- ================================================================================

-- Test 1: Event type distribution for patient e4BwD8ZYDBccepXcJ.Ilo3w3
/*
SELECT
    event_type,
    event_category,
    COUNT(*) as event_count,
    MIN(event_date) as earliest,
    MAX(event_date) as latest,
    COUNT(DISTINCT source_view) as source_view_count,
    LISTAGG(DISTINCT source_view, '; ') WITHIN GROUP (ORDER BY source_view) as source_views
FROM fhir_prd_db.v_unified_patient_timeline
WHERE patient_fhir_id = 'e4BwD8ZYDBccepXcJ.Ilo3w3'
GROUP BY event_type, event_category
ORDER BY event_count DESC;
*/

-- Test 2: Verify all CRITICAL gaps filled
/*
SELECT
    'Measurements' as gap_category,
    COUNT(*) as event_count
FROM fhir_prd_db.v_unified_patient_timeline
WHERE patient_fhir_id = 'e4BwD8ZYDBccepXcJ.Ilo3w3'
  AND event_type = 'Measurement'

UNION ALL

SELECT
    'Ophthalmology' as gap_category,
    COUNT(*) as event_count
FROM fhir_prd_db.v_unified_patient_timeline
WHERE patient_fhir_id = 'e4BwD8ZYDBccepXcJ.Ilo3w3'
  AND event_category = 'Ophthalmology'

UNION ALL

SELECT
    'Audiology' as gap_category,
    COUNT(*) as event_count
FROM fhir_prd_db.v_unified_patient_timeline
WHERE patient_fhir_id = 'e4BwD8ZYDBccepXcJ.Ilo3w3'
  AND event_category = 'Audiology'

UNION ALL

SELECT
    'Transplant' as gap_category,
    COUNT(*) as event_count
FROM fhir_prd_db.v_unified_patient_timeline
WHERE patient_fhir_id = 'e4BwD8ZYDBccepXcJ.Ilo3w3'
  AND event_category = 'Stem Cell Transplant'

UNION ALL

SELECT
    'Radiation Fractions' as gap_category,
    COUNT(*) as event_count
FROM fhir_prd_db.v_unified_patient_timeline
WHERE patient_fhir_id = 'e4BwD8ZYDBccepXcJ.Ilo3w3'
  AND event_type = 'Radiation Fraction';
*/

-- Test 3: Provenance tracking verification
/*
SELECT
    source_view,
    source_domain,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT patient_fhir_id) as patient_count
FROM fhir_prd_db.v_unified_patient_timeline
GROUP BY source_view, source_domain, event_type
ORDER BY source_view, event_type;
*/

-- Test 4: Check for duplicate events
/*
SELECT
    event_id,
    COUNT(*) as duplicate_count,
    MIN(event_date) as event_date,
    MIN(event_type) as event_type,
    MIN(source_view) as source_view
FROM fhir_prd_db.v_unified_patient_timeline
GROUP BY event_id
HAVING COUNT(*) > 1;
-- EXPECTED: 0 rows (no duplicates)
*/

-- Test 5: Extraction context verification (for agent queries)
/*
SELECT
    event_type,
    event_category,
    JSON_EXTRACT_SCALAR(extraction_context, '$.source_view') as source_view,
    JSON_EXTRACT_SCALAR(extraction_context, '$.requires_free_text_extraction') as needs_extraction,
    COUNT(*) as event_count
FROM fhir_prd_db.v_unified_patient_timeline
WHERE patient_fhir_id = 'e4BwD8ZYDBccepXcJ.Ilo3w3'
GROUP BY event_type, event_category,
    JSON_EXTRACT_SCALAR(extraction_context, '$.source_view'),
    JSON_EXTRACT_SCALAR(extraction_context, '$.requires_free_text_extraction')
ORDER BY event_type, event_category;
*/

;

-- ============================================================================
-- ============================================================================
-- EPISODE VIEW: Chemotherapy Treatment Episodes
-- ============================================================================
-- ============================================================================
-- Added: 2025-10-28
-- Purpose: Construct treatment episodes from medication data for:
--   1. Episode-level analysis and reporting
--   2. Note retrieval prioritization for document abstraction
--   3. Integration with patient timeline views
-- 
-- CRITICAL NOTE RETRIEVAL PRIORITIZATION GUIDANCE:
-- ==================================================
-- This view includes fields to guide optimization of clinical note retrieval
-- for document abstraction. The following prioritization logic is applied:
--
-- HIGH PRIORITY (retrieve notes immediately):
--   - Episodes with missing stop dates (may need completion documentation)
--   - Episodes with >3 unique drugs (complex regimens need verification)
--
-- MEDIUM PRIORITY (retrieve during systematic review):
--   - Episodes with medication_notes populated (additional context available)
--
-- LOW PRIORITY (routine abstraction):
--   - Standard episodes with complete structured data
--
-- How to use this view for note retrieval:
-- -----------------------------------------
-- 1. Use episode_encounter_reference to link to Encounter resources
-- 2. From Encounter, retrieve associated DocumentReference resources
-- 3. Prioritize note retrieval by note_retrieval_priority field
-- 4. Use note_retrieval_rationale to understand WHY notes are needed
-- 5. Focus abstraction efforts on HIGH priority episodes first
--
-- Example query for note retrieval prioritization:
-- /*
-- SELECT 
--     patient_fhir_id,
--     episode_id,
--     episode_encounter_reference,
--     note_retrieval_priority,
--     note_retrieval_rationale,
--     episode_drug_names,
--     episode_start_datetime,
--     episode_end_datetime
-- FROM fhir_prd_db.v_chemo_treatment_episodes
-- WHERE note_retrieval_priority = 'HIGH'
-- ORDER BY patient_fhir_id, episode_start_datetime;
-- */
--
-- Integration with Patient Timeline:
-- -----------------------------------
-- This episode view is designed to work alongside v_unified_patient_timeline
-- to provide comprehensive context for note abstraction:
--   - Episode view: Groups medications into treatment episodes
--   - Timeline view: Shows all clinical events around each episode
--   - Together: Enable targeted note retrieval at key clinical timepoints
--
-- Example integration query:
-- /*
-- SELECT 
--     e.patient_fhir_id,
--     e.episode_id,
--     e.episode_start_datetime,
--     e.note_retrieval_priority,
--     t.event_date,
--     t.event_type,
--     t.event_description
-- FROM fhir_prd_db.v_chemo_treatment_episodes e
-- INNER JOIN fhir_prd_db.v_unified_patient_timeline t
--     ON e.patient_fhir_id = t.patient_fhir_id
--     AND t.event_date BETWEEN e.episode_start_datetime AND COALESCE(e.episode_end_datetime, CURRENT_TIMESTAMP)
-- WHERE e.note_retrieval_priority = 'HIGH'
-- ORDER BY e.patient_fhir_id, e.episode_start_datetime, t.event_date;