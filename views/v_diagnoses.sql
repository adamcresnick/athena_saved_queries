CREATE OR REPLACE VIEW fhir_prd_db.v_diagnoses AS
SELECT
    patient_fhir_id,
    pld_condition_id as condition_id,
    pld_diagnosis_name as diagnosis_name,
    pld_clinical_status as clinical_status_text,
    pld_onset_date as onset_date_time,
    age_at_onset_days,
    pld_abatement_date as abatement_date_time,
    pld_recorded_date as recorded_date,
    age_at_recorded_days,
    pld_icd10_code as icd10_code,
    pld_icd10_display as icd10_display,
    TRY_CAST(pld_snomed_code AS BIGINT) as snomed_code,
    pld_snomed_display as snomed_display
FROM fhir_prd_db.v_problem_list_diagnoses;