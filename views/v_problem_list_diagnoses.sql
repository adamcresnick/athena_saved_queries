CREATE OR REPLACE VIEW fhir_prd_db.v_problem_list_diagnoses AS
SELECT
    pld.patient_id as patient_fhir_id,
    pld.condition_id as pld_condition_id,
    pld.diagnosis_name as pld_diagnosis_name,
    pld.clinical_status_text as pld_clinical_status,
    TRY(CAST(CASE
        WHEN LENGTH(pld.onset_date_time) = 10 THEN pld.onset_date_time || 'T00:00:00Z'
        ELSE pld.onset_date_time
    END AS TIMESTAMP(3))) as pld_onset_date,
    TRY(DATE_DIFF('day',
        DATE(pa.birth_date),
        CAST(SUBSTR(pld.onset_date_time, 1, 10) AS DATE))) as age_at_onset_days,
    TRY(CAST(CASE
        WHEN LENGTH(pld.abatement_date_time) = 10 THEN pld.abatement_date_time || 'T00:00:00Z'
        ELSE pld.abatement_date_time
    END AS TIMESTAMP(3))) as pld_abatement_date,
    TRY(CAST(CASE
        WHEN LENGTH(pld.recorded_date) = 10 THEN pld.recorded_date || 'T00:00:00Z'
        ELSE pld.recorded_date
    END AS TIMESTAMP(3))) as pld_recorded_date,
    TRY(DATE_DIFF('day',
        DATE(pa.birth_date),
        CAST(SUBSTR(pld.recorded_date, 1, 10) AS DATE))) as age_at_recorded_days,
    pld.icd10_code as pld_icd10_code,
    pld.icd10_display as pld_icd10_display,
    pld.snomed_code as pld_snomed_code,
    pld.snomed_display as pld_snomed_display
FROM fhir_prd_db.problem_list_diagnoses pld
LEFT JOIN fhir_prd_db.patient_access pa ON pld.patient_id = pa.id
WHERE pld.patient_id IS NOT NULL
ORDER BY pld.patient_id, pld.recorded_date;