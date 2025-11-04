CREATE OR REPLACE VIEW fhir_prd_db.v_hydrocephalus_diagnosis AS
WITH
-- All hydrocephalus conditions from condition table (5,735 records vs 427 in problem_list)
hydro_conditions AS (
    SELECT
        c.id as condition_id,
        c.subject_reference as patient_fhir_id,
        ccc.code_coding_code as icd10_code,
        ccc.code_coding_display as diagnosis_display,
        ccc.code_coding_system as code_system,
        c.code_text as condition_text,
        c.onset_date_time,
        c.onset_period_start,
        c.onset_period_end,
        c.abatement_date_time,
        c.recorded_date,
        c.clinical_status_text,
        c.verification_status_text,

        -- Hydrocephalus type classification from ICD-10
        CASE
            WHEN ccc.code_coding_code LIKE 'G91.0%' THEN 'Communicating'
            WHEN ccc.code_coding_code LIKE 'G91.1%' THEN 'Obstructive'
            WHEN ccc.code_coding_code LIKE 'G91.2%' THEN 'Normal-pressure'
            WHEN ccc.code_coding_code LIKE 'G91.3%' THEN 'Post-traumatic'
            WHEN ccc.code_coding_code LIKE 'G91.8%' THEN 'Other'
            WHEN ccc.code_coding_code LIKE 'G91.9%' THEN 'Unspecified'
            WHEN ccc.code_coding_code LIKE 'Q03%' THEN 'Congenital'
            ELSE 'Unclassified'
        END as hydrocephalus_type

    FROM fhir_prd_db.condition c
    INNER JOIN fhir_prd_db.condition_code_coding ccc
        ON c.id = ccc.condition_id
    WHERE (
        ccc.code_coding_code LIKE 'G91%'
        OR ccc.code_coding_code LIKE 'Q03%'
        OR LOWER(ccc.code_coding_display) LIKE '%hydroceph%'
    )
),

-- Diagnosis category classification (Problem List, Encounter, Admission, etc.)
condition_categories AS (
    SELECT
        cc.condition_id,
        LISTAGG(DISTINCT cc.category_text, ' | ') WITHIN GROUP (ORDER BY cc.category_text) as category_types,

        -- Is this an active/current condition?
        MAX(CASE
            WHEN cc.category_text IN ('Problem List Item', 'Encounter Diagnosis', 'Admission Diagnosis') THEN true
            ELSE false
        END) as is_active_diagnosis,

        -- Individual category flags
        MAX(CASE WHEN cc.category_text = 'Problem List Item' THEN true ELSE false END) as is_problem_list,
        MAX(CASE WHEN cc.category_text = 'Encounter Diagnosis' THEN true ELSE false END) as is_encounter_diagnosis,
        MAX(CASE WHEN cc.category_text = 'Admission Diagnosis' THEN true ELSE false END) as is_admission_diagnosis,
        MAX(CASE WHEN cc.category_text = 'Discharge Diagnosis' THEN true ELSE false END) as is_discharge_diagnosis,
        MAX(CASE WHEN cc.category_text = 'Medical History' THEN true ELSE false END) as is_medical_history

    FROM fhir_prd_db.condition_category cc
    WHERE cc.condition_id IN (SELECT condition_id FROM hydro_conditions)
    GROUP BY cc.condition_id
),

-- Imaging studies documenting hydrocephalus
hydro_imaging AS (
    SELECT
        dr.subject_reference as patient_fhir_id,
        dr.id as report_id,
        dr.code_text as study_type,
        dr.effective_date_time as imaging_date,
        dr.conclusion,

        -- Imaging modality classification
        CASE
            WHEN LOWER(dr.code_text) LIKE '%ct%'
                 OR LOWER(dr.code_text) LIKE '%computed%tomography%' THEN 'CT'
            WHEN LOWER(dr.code_text) LIKE '%mri%'
                 OR LOWER(dr.code_text) LIKE '%magnetic%resonance%' THEN 'MRI'
            WHEN LOWER(dr.code_text) LIKE '%ultrasound%' THEN 'Ultrasound'
            ELSE 'Other'
        END as imaging_modality

    FROM fhir_prd_db.diagnostic_report dr
    WHERE dr.subject_reference IS NOT NULL
      AND (
          LOWER(dr.code_text) LIKE '%brain%'
          OR LOWER(dr.code_text) LIKE '%head%'
          OR LOWER(dr.code_text) LIKE '%cranial%'
      )
      AND (
          LOWER(dr.conclusion) LIKE '%hydroceph%'
          OR LOWER(dr.conclusion) LIKE '%ventriculomegaly%'
          OR LOWER(dr.conclusion) LIKE '%enlarged%ventricle%'
          OR LOWER(dr.conclusion) LIKE '%ventricular%dilation%'
      )
),

-- Aggregate imaging per patient
imaging_summary AS (
    SELECT
        patient_fhir_id,
        LISTAGG(DISTINCT imaging_modality, ' | ') WITHIN GROUP (ORDER BY imaging_modality) as imaging_modalities,
        COUNT(DISTINCT report_id) as total_imaging_studies,
        COUNT(DISTINCT CASE WHEN imaging_modality = 'CT' THEN report_id END) as ct_studies,
        COUNT(DISTINCT CASE WHEN imaging_modality = 'MRI' THEN report_id END) as mri_studies,
        COUNT(DISTINCT CASE WHEN imaging_modality = 'Ultrasound' THEN report_id END) as ultrasound_studies,
        MIN(imaging_date) as first_imaging_date,
        MAX(imaging_date) as most_recent_imaging_date
    FROM hydro_imaging
    GROUP BY patient_fhir_id
),

-- Service requests for hydrocephalus imaging (validates diagnosis method)
imaging_orders AS (
    SELECT
        sr.subject_reference as patient_fhir_id,
        COUNT(DISTINCT sr.id) as total_imaging_orders,
        COUNT(DISTINCT CASE WHEN LOWER(sr.code_text) LIKE '%ct%' THEN sr.id END) as ct_orders,
        COUNT(DISTINCT CASE WHEN LOWER(sr.code_text) LIKE '%mri%' THEN sr.id END) as mri_orders,
        MIN(sr.occurrence_date_time) as first_order_date

    FROM fhir_prd_db.service_request sr
    INNER JOIN fhir_prd_db.service_request_reason_code src
        ON sr.id = src.service_request_id
    WHERE (
        LOWER(sr.code_text) LIKE '%ct%'
        OR LOWER(sr.code_text) LIKE '%mri%'
        OR LOWER(sr.code_text) LIKE '%ultrasound%'
    )
    AND (
        LOWER(src.reason_code_text) LIKE '%hydroceph%'
        OR LOWER(src.reason_code_text) LIKE '%ventriculomegaly%'
        OR LOWER(src.reason_code_text) LIKE '%increased%intracranial%pressure%'
    )
    GROUP BY sr.subject_reference
)

-- Main SELECT: Combine all diagnosis data
SELECT
    -- Patient identifier
    hc.patient_fhir_id,

    -- Condition fields (cond_ prefix)
    hc.condition_id as cond_id,
    hc.icd10_code as cond_icd10_code,
    hc.diagnosis_display as cond_diagnosis_display,
    hc.condition_text as cond_text,
    hc.code_system as cond_code_system,
    hc.hydrocephalus_type as cond_hydro_type,
    hc.clinical_status_text as cond_clinical_status,
    hc.verification_status_text as cond_verification_status,

    -- All date fields
    TRY(CAST(hc.onset_date_time AS TIMESTAMP(3))) as cond_onset_datetime,
    TRY(CAST(hc.onset_period_start AS TIMESTAMP(3))) as cond_onset_period_start,
    TRY(CAST(hc.onset_period_end AS TIMESTAMP(3))) as cond_onset_period_end,
    TRY(CAST(hc.abatement_date_time AS TIMESTAMP(3))) as cond_abatement_datetime,
    TRY(CAST(hc.recorded_date AS TIMESTAMP(3))) as cond_recorded_date,

    -- Diagnosis category fields (cat_ prefix)
    cc.category_types as cat_all_categories,
    cc.is_active_diagnosis as cat_is_active,
    cc.is_problem_list as cat_is_problem_list,
    cc.is_encounter_diagnosis as cat_is_encounter_dx,
    cc.is_admission_diagnosis as cat_is_admission_dx,
    cc.is_discharge_diagnosis as cat_is_discharge_dx,
    cc.is_medical_history as cat_is_medical_history,

    -- Imaging summary fields (img_ prefix)
    img.imaging_modalities as img_modalities,
    img.total_imaging_studies as img_total_studies,
    img.ct_studies as img_ct_count,
    img.mri_studies as img_mri_count,
    img.ultrasound_studies as img_ultrasound_count,
    TRY(CAST(img.first_imaging_date AS TIMESTAMP(3))) as img_first_date,
    TRY(CAST(img.most_recent_imaging_date AS TIMESTAMP(3))) as img_most_recent_date,

    -- Service request fields (sr_ prefix)
    io.total_imaging_orders as sr_total_orders,
    io.ct_orders as sr_ct_orders,
    io.mri_orders as sr_mri_orders,
    TRY(CAST(io.first_order_date AS TIMESTAMP(3))) as sr_first_order_date,

    -- ============================================================================
    -- CBTN FIELD MAPPINGS
    -- ============================================================================

    -- hydro_yn (always true for this view)
    true as hydro_yn,

    -- hydro_event_date (onset date)
    TRY(CAST(COALESCE(hc.onset_date_time, hc.onset_period_start, hc.recorded_date) AS TIMESTAMP(3))) as hydro_event_date,

    -- hydro_method_diagnosed (CT, MRI, Clinical, Other)
    CASE
        WHEN img.ct_studies > 0 AND img.mri_studies > 0 THEN 'CT and MRI'
        WHEN img.ct_studies > 0 THEN 'CT'
        WHEN img.mri_studies > 0 THEN 'MRI'
        WHEN img.ultrasound_studies > 0 THEN 'Ultrasound'
        WHEN img.total_imaging_studies > 0 THEN 'Imaging (Other)'
        ELSE 'Clinical'
    END as hydro_method_diagnosed,

    -- medical_conditions_present_at_event(11) - Hydrocephalus checkbox
    CASE
        WHEN cc.is_active_diagnosis = true THEN true
        WHEN hc.clinical_status_text = 'active' THEN true
        ELSE false
    END as medical_condition_hydrocephalus_present,

    -- ============================================================================
    -- DATA QUALITY INDICATORS
    -- ============================================================================

    CASE WHEN hc.icd10_code IS NOT NULL THEN true ELSE false END as has_icd10_code,
    CASE WHEN hc.onset_date_time IS NOT NULL OR hc.onset_period_start IS NOT NULL THEN true ELSE false END as has_onset_date,
    CASE WHEN img.total_imaging_studies > 0 THEN true ELSE false END as has_imaging_documentation,
    CASE WHEN io.total_imaging_orders > 0 THEN true ELSE false END as has_imaging_orders,
    CASE WHEN hc.verification_status_text = 'confirmed' THEN true ELSE false END as is_confirmed_diagnosis,
    CASE WHEN cc.is_problem_list = true THEN true ELSE false END as on_problem_list

FROM hydro_conditions hc
LEFT JOIN condition_categories cc ON hc.condition_id = cc.condition_id
LEFT JOIN imaging_summary img ON hc.patient_fhir_id = img.patient_fhir_id
LEFT JOIN imaging_orders io ON hc.patient_fhir_id = io.patient_fhir_id

ORDER BY hc.patient_fhir_id, hc.onset_date_time;