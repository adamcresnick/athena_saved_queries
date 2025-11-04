CREATE OR REPLACE VIEW fhir_prd_db.v_encounters AS
WITH encounter_types_agg AS (
    SELECT
        encounter_id,
        LISTAGG(type_coding, '; ') WITHIN GROUP (ORDER BY type_coding) as type_coding,
        LISTAGG(type_text, '; ') WITHIN GROUP (ORDER BY type_text) as type_text
    FROM (
        SELECT DISTINCT encounter_id, type_coding, type_text
        FROM fhir_prd_db.encounter_type
    )
    GROUP BY encounter_id
),
encounter_reasons_agg AS (
    SELECT
        encounter_id,
        LISTAGG(reason_code_coding, '; ') WITHIN GROUP (ORDER BY reason_code_coding) as reason_code_coding,
        LISTAGG(reason_code_text, '; ') WITHIN GROUP (ORDER BY reason_code_text) as reason_code_text
    FROM (
        SELECT DISTINCT encounter_id, reason_code_coding, reason_code_text
        FROM fhir_prd_db.encounter_reason_code
    )
    GROUP BY encounter_id
),
encounter_diagnoses_agg AS (
    SELECT
        encounter_id,
        LISTAGG(diagnosis_condition_reference, '; ') WITHIN GROUP (ORDER BY diagnosis_condition_reference) as diagnosis_condition_reference,
        LISTAGG(diagnosis_condition_display, '; ') WITHIN GROUP (ORDER BY diagnosis_condition_display) as diagnosis_condition_display,
        LISTAGG(diagnosis_use_coding, '; ') WITHIN GROUP (ORDER BY diagnosis_use_coding) as diagnosis_use_coding,
        LISTAGG(diagnosis_rank_str, '; ') WITHIN GROUP (ORDER BY diagnosis_rank_str) as diagnosis_rank
    FROM (
        SELECT DISTINCT
            encounter_id,
            diagnosis_condition_reference,
            diagnosis_condition_display,
            diagnosis_use_coding,
            CAST(diagnosis_rank AS VARCHAR) as diagnosis_rank_str
        FROM fhir_prd_db.encounter_diagnosis
    )
    GROUP BY encounter_id
),
encounter_appointments_agg AS (
    SELECT
        encounter_id,
        LISTAGG(appointment_reference, '; ') WITHIN GROUP (ORDER BY appointment_reference) as appointment_reference
    FROM (
        SELECT DISTINCT encounter_id, appointment_reference
        FROM fhir_prd_db.encounter_appointment
    )
    GROUP BY encounter_id
),
encounter_service_type_coding_agg AS (
    SELECT
        encounter_id,
        LISTAGG(service_type_coding_display, '; ') WITHIN GROUP (ORDER BY service_type_coding_display) as service_type_coding_display_detail
    FROM (
        SELECT DISTINCT encounter_id, service_type_coding_display
        FROM fhir_prd_db.encounter_service_type_coding
    )
    GROUP BY encounter_id
),
encounter_locations_agg AS (
    SELECT
        encounter_id,
        LISTAGG(location_location_reference, '; ') WITHIN GROUP (ORDER BY location_location_reference) as location_location_reference,
        LISTAGG(location_status, '; ') WITHIN GROUP (ORDER BY location_status) as location_status
    FROM (
        SELECT DISTINCT encounter_id, location_location_reference, location_status
        FROM fhir_prd_db.encounter_location
    )
    GROUP BY encounter_id
)
SELECT
    e.id as encounter_fhir_id,
    TRY(CAST(SUBSTR(e.period_start, 1, 10) AS DATE)) as encounter_date,
    TRY(DATE_DIFF('day',
        DATE(pa.birth_date),
        TRY(CAST(SUBSTR(e.period_start, 1, 10) AS DATE)))) as age_at_encounter_days,
    e.status,
    e.class_code,
    e.class_display,
    e.service_type_text,
    e.priority_text,
    COALESCE(
        TRY(date_parse(e.period_start, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(e.period_start, '%Y-%m-%d'))
    ) as period_start,
    COALESCE(
        TRY(date_parse(e.period_end, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(e.period_end, '%Y-%m-%d'))
    ) as period_end,
    e.length_value,
    e.length_unit,
    e.service_provider_display,
    e.part_of_reference,
    e.subject_reference as patient_fhir_id,

    -- Aggregated subtables (matching Python CSV output)
    et.type_coding,
    et.type_text,
    er.reason_code_coding,
    er.reason_code_text,
    ed.diagnosis_condition_reference,
    ed.diagnosis_condition_display,
    ed.diagnosis_use_coding,
    ed.diagnosis_rank,
    ea.appointment_reference,
    estc.service_type_coding_display_detail,
    el.location_location_reference,
    el.location_status,

    -- Patient type classification (matches Python logic)
    CASE
        WHEN LOWER(e.class_display) LIKE '%inpatient%' OR e.class_code = 'IMP' THEN 'Inpatient'
        WHEN LOWER(e.class_display) LIKE '%outpatient%' OR e.class_code = 'AMB'
             OR LOWER(e.class_display) LIKE '%appointment%' THEN 'Outpatient'
        ELSE 'Unknown'
    END as patient_type

FROM fhir_prd_db.encounter e
LEFT JOIN encounter_types_agg et ON e.id = et.encounter_id
LEFT JOIN encounter_reasons_agg er ON e.id = er.encounter_id
LEFT JOIN encounter_diagnoses_agg ed ON e.id = ed.encounter_id
LEFT JOIN encounter_appointments_agg ea ON e.id = ea.encounter_id
LEFT JOIN encounter_service_type_coding_agg estc ON e.id = estc.encounter_id
LEFT JOIN encounter_locations_agg el ON e.id = el.encounter_id
LEFT JOIN fhir_prd_db.patient_access pa ON e.subject_reference = pa.id
WHERE e.subject_reference IS NOT NULL
ORDER BY e.subject_reference, e.period_start;