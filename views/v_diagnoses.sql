CREATE OR REPLACE VIEW fhir_prd_db.v_diagnoses AS
SELECT
    pld.patient_fhir_id,
    pld.pld_condition_id as condition_id,
    pld.pld_diagnosis_name as diagnosis_name,
    pld.pld_clinical_status as clinical_status_text,
    pld.pld_onset_datetime as onset_date_time,
    TRY(DATE_DIFF('day',
        DATE(pa.birth_date),
        DATE(TRY(from_iso8601_timestamp(pld.pld_onset_datetime)))
    )) as age_at_onset_days,
    pld.pld_abatement_datetime as abatement_date_time,
    pld.pld_recorded_date as recorded_date,
    TRY(DATE_DIFF('day',
        DATE(pa.birth_date),
        DATE(TRY(from_iso8601_timestamp(pld.pld_recorded_date)))
    )) as age_at_recorded_days,
    pld.pld_icd10_code as icd10_code,
    pld.pld_icd10_display as icd10_display,
    TRY_CAST(pld.pld_snomed_code AS BIGINT) as snomed_code,
    pld.pld_snomed_display as snomed_display
FROM fhir_prd_db.v_problem_list_diagnoses pld
LEFT JOIN fhir_prd_db.patient_access pa ON pld.patient_fhir_id = pa.id;