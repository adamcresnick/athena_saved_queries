-- v_problem_list_diagnoses: Comprehensive SNOMED/ICD diagnoses from all FHIR sources
--
-- Creates a VIEW (does not replace problem_list_diagnoses table) that aggregates SNOMED CT
-- and ICD-10-CM codes from 11 FHIR tables with code_coding_system columns.
--
-- SOURCES: Condition (Problem List Items) + 10 additional FHIR resource types
-- SCOPE: Only SNOMED CT (http://snomed.info/sct) and ICD-10-CM codes (matching original table)
-- DEDUPLICATION: ROW_NUMBER() keeps earliest occurrence per patient + code combination
-- OUTPUT: Same schema as original problem_list_diagnoses table for backward compatibility
--
-- NOTE: Excludes v_procedures_tumor (materialized view) to avoid circular dependency
-- NOTE: This is a VIEW - the problem_list_diagnoses TABLE remains unchanged

CREATE OR REPLACE VIEW fhir_prd_db.v_problem_list_diagnoses AS

WITH

-- 1. Condition resources (original source - Problem List Items only)
condition_findings AS (
    SELECT
        c.subject_reference as patient_id,
        pa.mrn,
        c.id as condition_id,
        c.code_text as diagnosis_name,
        c.clinical_status_text,
        c.onset_date_time,
        c.abatement_date_time,
        c.recorded_date,
        icd10.code_coding_code as icd10_code,
        icd10.code_coding_display as icd10_display,
        snomed.code_coding_code as snomed_code,
        snomed.code_coding_display as snomed_display,
        -- For deduplication
        COALESCE(snomed.code_coding_code, icd10.code_coding_code) as code,
        CASE
            WHEN snomed.code_coding_code IS NOT NULL THEN 'http://snomed.info/sct'
            WHEN icd10.code_coding_code IS NOT NULL THEN 'http://hl7.org/fhir/sid/icd-10-cm'
        END as coding_system,
        COALESCE(
            TRY(date_parse(c.onset_date_time, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(c.recorded_date, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(c.onset_date_time, '%Y-%m-%d')),
            TRY(date_parse(c.recorded_date, '%Y-%m-%d'))
        ) as finding_datetime
    FROM fhir_prd_db.condition c
    INNER JOIN fhir_prd_db.patient_access pa ON c.subject_reference = pa.id
    LEFT JOIN fhir_prd_db.condition_category ccat ON c.id = ccat.condition_id
    LEFT JOIN fhir_prd_db.condition_code_coding as icd10 ON c.id = icd10.condition_id
        AND icd10.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm'
    LEFT JOIN fhir_prd_db.condition_code_coding as snomed ON c.id = snomed.condition_id
        AND snomed.code_coding_system = 'http://snomed.info/sct'
    WHERE ccat.category_text = 'Problem List Item'
        AND (snomed.code_coding_code IS NOT NULL OR icd10.code_coding_code IS NOT NULL)
),

-- 2. Procedure resources
procedure_findings AS (
    SELECT
        REPLACE(p.subject_reference, 'Patient/', '') as patient_id,
        pa.mrn,
        p.id as condition_id,
        p.code_text as diagnosis_name,
        p.status as clinical_status_text,
        p.performed_date_time as onset_date_time,
        NULL as abatement_date_time,
        p.performed_date_time as recorded_date,
        CASE WHEN pc.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN pc.code_coding_code END as icd10_code,
        CASE WHEN pc.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN pc.code_coding_display END as icd10_display,
        CASE WHEN pc.code_coding_system = 'http://snomed.info/sct' THEN pc.code_coding_code END as snomed_code,
        CASE WHEN pc.code_coding_system = 'http://snomed.info/sct' THEN pc.code_coding_display END as snomed_display,
        pc.code_coding_code as code,
        pc.code_coding_system as coding_system,
        COALESCE(
            TRY(date_parse(p.performed_date_time, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(p.performed_period_start, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(p.performed_date_time, '%Y-%m-%d'))
        ) as finding_datetime
    FROM fhir_prd_db.procedure p
    INNER JOIN fhir_prd_db.patient_access pa ON REPLACE(p.subject_reference, 'Patient/', '') = pa.id
    INNER JOIN fhir_prd_db.procedure_code_coding pc ON p.id = pc.procedure_id
    WHERE pc.code_coding_system IN ('http://snomed.info/sct', 'http://hl7.org/fhir/sid/icd-10-cm')
),

-- 3. Observation resources
observation_findings AS (
    SELECT
        REPLACE(o.subject_reference, 'Patient/', '') as patient_id,
        pa.mrn,
        o.id as condition_id,
        o.code_text as diagnosis_name,
        o.status as clinical_status_text,
        o.effective_date_time as onset_date_time,
        NULL as abatement_date_time,
        o.issued as recorded_date,
        CASE WHEN oc.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN oc.code_coding_code END as icd10_code,
        CASE WHEN oc.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN oc.code_coding_display END as icd10_display,
        CASE WHEN oc.code_coding_system = 'http://snomed.info/sct' THEN oc.code_coding_code END as snomed_code,
        CASE WHEN oc.code_coding_system = 'http://snomed.info/sct' THEN oc.code_coding_display END as snomed_display,
        oc.code_coding_code as code,
        oc.code_coding_system as coding_system,
        COALESCE(
            TRY(date_parse(o.effective_date_time, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(o.effective_period_start, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(o.issued, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(o.effective_date_time, '%Y-%m-%d'))
        ) as finding_datetime
    FROM fhir_prd_db.observation o
    INNER JOIN fhir_prd_db.patient_access pa ON REPLACE(o.subject_reference, 'Patient/', '') = pa.id
    INNER JOIN fhir_prd_db.observation_code_coding oc ON o.id = oc.observation_id
    WHERE oc.code_coding_system IN ('http://snomed.info/sct', 'http://hl7.org/fhir/sid/icd-10-cm')
),

-- 4. DiagnosticReport resources
diagnostic_report_findings AS (
    SELECT
        REPLACE(dr.subject_reference, 'Patient/', '') as patient_id,
        pa.mrn,
        dr.id as condition_id,
        dr.code_text as diagnosis_name,
        dr.status as clinical_status_text,
        dr.effective_date_time as onset_date_time,
        NULL as abatement_date_time,
        dr.issued as recorded_date,
        CASE WHEN drc.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN drc.code_coding_code END as icd10_code,
        CASE WHEN drc.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN drc.code_coding_display END as icd10_display,
        CASE WHEN drc.code_coding_system = 'http://snomed.info/sct' THEN drc.code_coding_code END as snomed_code,
        CASE WHEN drc.code_coding_system = 'http://snomed.info/sct' THEN drc.code_coding_display END as snomed_display,
        drc.code_coding_code as code,
        drc.code_coding_system as coding_system,
        COALESCE(
            TRY(date_parse(dr.effective_date_time, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(dr.issued, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(dr.effective_date_time, '%Y-%m-%d'))
        ) as finding_datetime
    FROM fhir_prd_db.diagnostic_report dr
    INNER JOIN fhir_prd_db.patient_access pa ON REPLACE(dr.subject_reference, 'Patient/', '') = pa.id
    INNER JOIN fhir_prd_db.diagnostic_report_code_coding drc ON dr.id = drc.diagnostic_report_id
    WHERE drc.code_coding_system IN ('http://snomed.info/sct', 'http://hl7.org/fhir/sid/icd-10-cm')
),

-- 5. MedicationRequest resources (requires 3-table join: medication_request → medication → medication_code_coding)
medication_findings AS (
    SELECT
        REPLACE(mr.subject_reference, 'Patient/', '') as patient_id,
        pa.mrn,
        mr.id as condition_id,
        COALESCE(med.code_text, mr.medication_codeable_concept_text) as diagnosis_name,
        mr.status as clinical_status_text,
        mr.authored_on as onset_date_time,
        NULL as abatement_date_time,
        mr.authored_on as recorded_date,
        CASE WHEN mcc.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN mcc.code_coding_code END as icd10_code,
        CASE WHEN mcc.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN mcc.code_coding_display END as icd10_display,
        CASE WHEN mcc.code_coding_system = 'http://snomed.info/sct' THEN mcc.code_coding_code END as snomed_code,
        CASE WHEN mcc.code_coding_system = 'http://snomed.info/sct' THEN mcc.code_coding_display END as snomed_display,
        mcc.code_coding_code as code,
        mcc.code_coding_system as coding_system,
        COALESCE(
            TRY(date_parse(mr.authored_on, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(mr.authored_on, '%Y-%m-%d'))
        ) as finding_datetime
    FROM fhir_prd_db.medication_request mr
    INNER JOIN fhir_prd_db.patient_access pa ON REPLACE(mr.subject_reference, 'Patient/', '') = pa.id
    INNER JOIN fhir_prd_db.medication med ON REPLACE(mr.medication_reference_reference, 'Medication/', '') = med.id
    INNER JOIN fhir_prd_db.medication_code_coding mcc ON med.id = mcc.medication_id
    WHERE mcc.code_coding_system IN ('http://snomed.info/sct', 'http://hl7.org/fhir/sid/icd-10-cm')
),

-- 6. AllergyIntolerance resources
allergy_findings AS (
    SELECT
        REPLACE(ai.patient_reference, 'Patient/', '') as patient_id,
        pa.mrn,
        ai.id as condition_id,
        ai.code_text as diagnosis_name,
        ai.clinical_status_text,
        ai.onset_date_time,
        NULL as abatement_date_time,
        ai.recorded_date,
        CASE WHEN ac.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN ac.code_coding_code END as icd10_code,
        CASE WHEN ac.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN ac.code_coding_display END as icd10_display,
        CASE WHEN ac.code_coding_system = 'http://snomed.info/sct' THEN ac.code_coding_code END as snomed_code,
        CASE WHEN ac.code_coding_system = 'http://snomed.info/sct' THEN ac.code_coding_display END as snomed_display,
        ac.code_coding_code as code,
        ac.code_coding_system as coding_system,
        COALESCE(
            TRY(date_parse(ai.onset_date_time, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(ai.recorded_date, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(ai.onset_date_time, '%Y-%m-%d'))
        ) as finding_datetime
    FROM fhir_prd_db.allergy_intolerance ai
    INNER JOIN fhir_prd_db.patient_access pa ON REPLACE(ai.patient_reference, 'Patient/', '') = pa.id
    INNER JOIN fhir_prd_db.allergy_intolerance_code_coding ac ON ai.id = ac.allergy_intolerance_id
    WHERE ac.code_coding_system IN ('http://snomed.info/sct', 'http://hl7.org/fhir/sid/icd-10-cm')
),

-- 7. Immunization resources
immunization_findings AS (
    SELECT
        REPLACE(i.patient_reference, 'Patient/', '') as patient_id,
        pa.mrn,
        i.id as condition_id,
        i.vaccine_code_text as diagnosis_name,
        i.status as clinical_status_text,
        i.occurrence_date_time as onset_date_time,
        NULL as abatement_date_time,
        i.recorded as recorded_date,
        CASE WHEN ic.vaccine_code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN ic.vaccine_code_coding_code END as icd10_code,
        CASE WHEN ic.vaccine_code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN ic.vaccine_code_coding_display END as icd10_display,
        CASE WHEN ic.vaccine_code_coding_system = 'http://snomed.info/sct' THEN ic.vaccine_code_coding_code END as snomed_code,
        CASE WHEN ic.vaccine_code_coding_system = 'http://snomed.info/sct' THEN ic.vaccine_code_coding_display END as snomed_display,
        ic.vaccine_code_coding_code as code,
        ic.vaccine_code_coding_system as coding_system,
        COALESCE(
            TRY(date_parse(i.occurrence_date_time, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(i.recorded, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(i.occurrence_date_time, '%Y-%m-%d'))
        ) as finding_datetime
    FROM fhir_prd_db.immunization i
    INNER JOIN fhir_prd_db.patient_access pa ON REPLACE(i.patient_reference, 'Patient/', '') = pa.id
    INNER JOIN fhir_prd_db.immunization_vaccine_code_coding ic ON i.id = ic.immunization_id
    WHERE ic.vaccine_code_coding_system IN ('http://snomed.info/sct', 'http://hl7.org/fhir/sid/icd-10-cm')
),

-- 8. ServiceRequest resources
service_request_findings AS (
    SELECT
        REPLACE(sr.subject_reference, 'Patient/', '') as patient_id,
        pa.mrn,
        sr.id as condition_id,
        sr.code_text as diagnosis_name,
        sr.status as clinical_status_text,
        sr.authored_on as onset_date_time,
        NULL as abatement_date_time,
        sr.authored_on as recorded_date,
        CASE WHEN src.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN src.code_coding_code END as icd10_code,
        CASE WHEN src.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN src.code_coding_display END as icd10_display,
        CASE WHEN src.code_coding_system = 'http://snomed.info/sct' THEN src.code_coding_code END as snomed_code,
        CASE WHEN src.code_coding_system = 'http://snomed.info/sct' THEN src.code_coding_display END as snomed_display,
        src.code_coding_code as code,
        src.code_coding_system as coding_system,
        COALESCE(
            TRY(date_parse(sr.authored_on, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(sr.authored_on, '%Y-%m-%d'))
        ) as finding_datetime
    FROM fhir_prd_db.service_request sr
    INNER JOIN fhir_prd_db.patient_access pa ON REPLACE(sr.subject_reference, 'Patient/', '') = pa.id
    INNER JOIN fhir_prd_db.service_request_code_coding src ON sr.id = src.service_request_id
    WHERE src.code_coding_system IN ('http://snomed.info/sct', 'http://hl7.org/fhir/sid/icd-10-cm')
),

-- 9. List resources
list_findings AS (
    SELECT
        REPLACE(l.subject_reference, 'Patient/', '') as patient_id,
        pa.mrn,
        l.id as condition_id,
        l.title as diagnosis_name,
        l.status as clinical_status_text,
        l.date as onset_date_time,
        NULL as abatement_date_time,
        l.date as recorded_date,
        CASE WHEN lc.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN lc.code_coding_code END as icd10_code,
        CASE WHEN lc.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN lc.code_coding_display END as icd10_display,
        CASE WHEN lc.code_coding_system = 'http://snomed.info/sct' THEN lc.code_coding_code END as snomed_code,
        CASE WHEN lc.code_coding_system = 'http://snomed.info/sct' THEN lc.code_coding_display END as snomed_display,
        lc.code_coding_code as code,
        lc.code_coding_system as coding_system,
        COALESCE(
            TRY(date_parse(l.date, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(l.date, '%Y-%m-%d'))
        ) as finding_datetime
    FROM fhir_prd_db.list l
    INNER JOIN fhir_prd_db.patient_access pa ON REPLACE(l.subject_reference, 'Patient/', '') = pa.id
    INNER JOIN fhir_prd_db.list_code_coding lc ON l.id = lc.list_id
    WHERE lc.code_coding_system IN ('http://snomed.info/sct', 'http://hl7.org/fhir/sid/icd-10-cm')
),

-- 10. RequestGroup resources
request_group_findings AS (
    SELECT
        REPLACE(rg.subject_reference, 'Patient/', '') as patient_id,
        pa.mrn,
        rg.id as condition_id,
        rg.code_text as diagnosis_name,
        rg.status as clinical_status_text,
        rg.authored_on as onset_date_time,
        NULL as abatement_date_time,
        rg.authored_on as recorded_date,
        CASE WHEN rgc.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN rgc.code_coding_code END as icd10_code,
        CASE WHEN rgc.code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN rgc.code_coding_display END as icd10_display,
        CASE WHEN rgc.code_coding_system = 'http://snomed.info/sct' THEN rgc.code_coding_code END as snomed_code,
        CASE WHEN rgc.code_coding_system = 'http://snomed.info/sct' THEN rgc.code_coding_display END as snomed_display,
        rgc.code_coding_code as code,
        rgc.code_coding_system as coding_system,
        COALESCE(
            TRY(date_parse(rg.authored_on, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(rg.authored_on, '%Y-%m-%d'))
        ) as finding_datetime
    FROM fhir_prd_db.request_group rg
    INNER JOIN fhir_prd_db.patient_access pa ON REPLACE(rg.subject_reference, 'Patient/', '') = pa.id
    INNER JOIN fhir_prd_db.request_group_code_coding rgc ON rg.id = rgc.request_group_id
    WHERE rgc.code_coding_system IN ('http://snomed.info/sct', 'http://hl7.org/fhir/sid/icd-10-cm')
),

-- 11. Observation timing codes
observation_timing_findings AS (
    SELECT
        REPLACE(o.subject_reference, 'Patient/', '') as patient_id,
        pa.mrn,
        o.id as condition_id,
        o.code_text as diagnosis_name,
        o.status as clinical_status_text,
        o.effective_date_time as onset_date_time,
        NULL as abatement_date_time,
        o.issued as recorded_date,
        CASE WHEN otc.effective_timing_code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN otc.effective_timing_code_coding_code END as icd10_code,
        CASE WHEN otc.effective_timing_code_coding_system = 'http://hl7.org/fhir/sid/icd-10-cm' THEN otc.effective_timing_code_coding_display END as icd10_display,
        CASE WHEN otc.effective_timing_code_coding_system = 'http://snomed.info/sct' THEN otc.effective_timing_code_coding_code END as snomed_code,
        CASE WHEN otc.effective_timing_code_coding_system = 'http://snomed.info/sct' THEN otc.effective_timing_code_coding_display END as snomed_display,
        otc.effective_timing_code_coding_code as code,
        otc.effective_timing_code_coding_system as coding_system,
        COALESCE(
            TRY(date_parse(o.effective_date_time, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(o.effective_period_start, '%Y-%m-%dT%H:%i:%sZ')),
            TRY(date_parse(o.effective_date_time, '%Y-%m-%d'))
        ) as finding_datetime
    FROM fhir_prd_db.observation o
    INNER JOIN fhir_prd_db.patient_access pa ON REPLACE(o.subject_reference, 'Patient/', '') = pa.id
    INNER JOIN fhir_prd_db.observation_effective_timing_code_coding otc ON o.id = otc.observation_id
    WHERE otc.effective_timing_code_coding_system IN ('http://snomed.info/sct', 'http://hl7.org/fhir/sid/icd-10-cm')
),

-- Union all sources (11 tables - excluding v_procedures_tumor to avoid circular dependency)
all_findings AS (
    SELECT * FROM condition_findings
    UNION ALL
    SELECT * FROM procedure_findings
    UNION ALL
    SELECT * FROM observation_findings
    UNION ALL
    SELECT * FROM diagnostic_report_findings
    UNION ALL
    SELECT * FROM medication_findings
    UNION ALL
    SELECT * FROM allergy_findings
    UNION ALL
    SELECT * FROM immunization_findings
    UNION ALL
    SELECT * FROM service_request_findings
    UNION ALL
    SELECT * FROM list_findings
    UNION ALL
    SELECT * FROM request_group_findings
    UNION ALL
    SELECT * FROM observation_timing_findings
)

-- Deduplicate and output with original schema
SELECT
    patient_id as patient_fhir_id,
    mrn,
    condition_id as pld_condition_id,
    diagnosis_name as pld_diagnosis_name,
    clinical_status_text as pld_clinical_status,
    onset_date_time as pld_onset_datetime,
    abatement_date_time as pld_abatement_datetime,
    recorded_date as pld_recorded_date,
    icd10_code as pld_icd10_code,
    icd10_display as pld_icd10_display,
    snomed_code as pld_snomed_code,
    snomed_display as pld_snomed_display
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id, code, coding_system
            ORDER BY finding_datetime ASC NULLS LAST, condition_id
        ) as rn
    FROM all_findings
    WHERE code IS NOT NULL
) ranked
WHERE rn = 1
ORDER BY patient_id, recorded_date DESC NULLS LAST;
