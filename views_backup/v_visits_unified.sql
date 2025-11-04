CREATE OR REPLACE VIEW fhir_prd_db.v_visits_unified AS
WITH appointment_encounter_links AS (
    -- Map appointments to their corresponding encounters
    SELECT DISTINCT
        SUBSTRING(ea.appointment_reference, 13) as appointment_id,  -- Remove "Appointment/" prefix
        ea.encounter_id,
        e.status as encounter_status,
        CAST(FROM_ISO8601_TIMESTAMP(e.period_start) AS TIMESTAMP(3)) as encounter_start,
        CAST(FROM_ISO8601_TIMESTAMP(e.period_end) AS TIMESTAMP(3)) as encounter_end
    FROM fhir_prd_db.encounter_appointment ea
    LEFT JOIN fhir_prd_db.encounter e ON ea.encounter_id = e.id
),
appointments_with_encounters AS (
    SELECT
        CAST(a.id AS VARCHAR) as appointment_fhir_id,
        CAST(ap.participant_actor_reference AS VARCHAR) as patient_fhir_id,

        -- Appointment details (explicit casts for UNION compatibility)
        CAST(a.status AS VARCHAR) as appointment_status,
        CAST(a.appointment_type_text AS VARCHAR) as appointment_type_text,
        TRY(CAST(CAST(a.start AS VARCHAR) AS TIMESTAMP(3))) as appointment_start,
        TRY(CAST(CAST(a."end" AS VARCHAR) AS TIMESTAMP(3))) as appointment_end,
        CAST(a.minutes_duration AS VARCHAR) as appointment_duration_minutes,
        CAST(a.cancelation_reason_text AS VARCHAR) as cancelation_reason_text,
        CAST(a.description AS VARCHAR) as appointment_description,

        -- Linked encounter details (already TIMESTAMP from CTE, just cast again for consistency)
        CAST(ael.encounter_id AS VARCHAR) as encounter_id,
        CAST(ael.encounter_status AS VARCHAR) as encounter_status,
        ael.encounter_start,
        ael.encounter_end,

        -- Visit type classification
        CASE
            WHEN a.status = 'fulfilled' AND ael.encounter_id IS NOT NULL THEN 'completed_scheduled'
            WHEN a.status = 'fulfilled' AND ael.encounter_id IS NULL THEN 'completed_no_encounter'
            WHEN a.status = 'noshow' THEN 'no_show'
            WHEN a.status = 'cancelled' THEN 'cancelled'
            WHEN a.status IN ('booked', 'pending', 'proposed') THEN 'future_scheduled'
            ELSE 'other'
        END as visit_type,

        -- Completion flags
        CASE WHEN a.status = 'fulfilled' THEN true ELSE false END as appointment_completed,
        CASE WHEN ael.encounter_id IS NOT NULL THEN true ELSE false END as encounter_occurred,

        -- Source indicator
        'appointment' as source

    FROM fhir_prd_db.appointment a
    JOIN fhir_prd_db.appointment_participant ap ON a.id = ap.appointment_id
    LEFT JOIN appointment_encounter_links ael ON a.id = ael.appointment_id
    WHERE ap.participant_actor_reference LIKE 'Patient/%'
),
encounters_without_appointments AS (
    -- Find encounters that don't have a linked appointment (walk-ins, emergency, etc.)
    SELECT
        CAST(NULL AS VARCHAR) as appointment_fhir_id,
        CAST(e.subject_reference AS VARCHAR) as patient_fhir_id,

        -- Appointment details (NULL for walk-ins) - types must match appointments_with_encounters
        CAST(NULL AS VARCHAR) as appointment_status,
        CAST(NULL AS VARCHAR) as appointment_type_text,
        TRY(CAST(CAST(NULL AS VARCHAR) AS TIMESTAMP(3))) as appointment_start,
        TRY(CAST(CAST(NULL AS VARCHAR) AS TIMESTAMP(3))) as appointment_end,
        CAST(NULL AS VARCHAR) as appointment_duration_minutes,
        CAST(NULL AS VARCHAR) as cancelation_reason_text,
        CAST(NULL AS VARCHAR) as appointment_description,

        -- Encounter details
        CAST(e.id AS VARCHAR) as encounter_id,
        CAST(e.status AS VARCHAR) as encounter_status,
        CAST(FROM_ISO8601_TIMESTAMP(e.period_start) AS TIMESTAMP(3)) as encounter_start,
        CAST(FROM_ISO8601_TIMESTAMP(e.period_end) AS TIMESTAMP(3)) as encounter_end,

        -- Visit type
        'walk_in_unscheduled' as visit_type,

        -- Completion flags
        CAST(false AS BOOLEAN) as appointment_completed,
        CAST(true AS BOOLEAN) as encounter_occurred,

        -- Source indicator
        'encounter' as source

    FROM fhir_prd_db.encounter e
    WHERE e.subject_reference IS NOT NULL
      AND e.id NOT IN (
          SELECT encounter_id
          FROM fhir_prd_db.encounter_appointment
          WHERE encounter_id IS NOT NULL
      )
),
combined_visits AS (
    SELECT * FROM appointments_with_encounters
    UNION ALL
    SELECT * FROM encounters_without_appointments
),
deduplicated_visits AS (
    -- Assign canonical visit_id and use ROW_NUMBER to pick primary row
    SELECT
        *,
        -- Canonical visit key (prefer encounter_id, fallback to appointment_id with suffix)
        COALESCE(encounter_id, appointment_fhir_id || '_appt') as visit_id,
        -- Use ROW_NUMBER to pick primary row when multiple linkages exist
        ROW_NUMBER() OVER (
            PARTITION BY patient_fhir_id, COALESCE(encounter_id, appointment_fhir_id || '_appt')
            ORDER BY
                CASE WHEN encounter_id IS NOT NULL THEN 1 ELSE 2 END,  -- Prefer rows with encounters
                CASE WHEN appointment_fhir_id IS NOT NULL THEN 1 ELSE 2 END,  -- Then with appointments
                COALESCE(appointment_start, encounter_start) DESC  -- Then most recent
        ) as row_rank
    FROM combined_visits
)
-- Combine both appointment-based and walk-in encounters
SELECT
    dv.patient_fhir_id,

    -- Visit identifiers
    dv.visit_id,
    dv.appointment_fhir_id,
    dv.encounter_id,

    -- Visit classification
    dv.visit_type,
    dv.appointment_completed,
    dv.encounter_occurred,
    dv.source,

    -- Appointment details
    dv.appointment_status,
    dv.appointment_type_text,
    dv.appointment_start,
    dv.appointment_end,
    dv.appointment_duration_minutes,
    dv.cancelation_reason_text,
    dv.appointment_description,

    -- Encounter details
    dv.encounter_status,
    dv.encounter_start,
    dv.encounter_end,

    -- Calculate age at visit (use appointment or encounter date)
    TRY(DATE_DIFF('day',
        DATE(pa.birth_date),
        DATE(COALESCE(dv.appointment_start, dv.encounter_start)))) as age_at_visit_days,

    -- Visit date (earliest of appointment or encounter)
    DATE(COALESCE(dv.appointment_start, dv.encounter_start)) as visit_date

FROM deduplicated_visits dv
LEFT JOIN fhir_prd_db.patient_access pa ON dv.patient_fhir_id = pa.id
WHERE dv.row_rank = 1  -- Only keep primary row per visit

ORDER BY dv.patient_fhir_id, visit_date, dv.appointment_start, dv.encounter_start;