CREATE OR REPLACE VIEW fhir_prd_db.v_appointments AS
SELECT DISTINCT
    a.id as appointment_fhir_id,
    TRY(CAST(SUBSTR(NULLIF(a.start, ''), 1, 10) AS DATE)) as appointment_date,
    TRY(DATE_DIFF('day',
        DATE(pa.birth_date),
        TRY(CAST(SUBSTR(NULLIF(a.start, ''), 1, 10) AS DATE)))) as age_at_appointment_days,

    -- Appointment fields (appt_ prefix matching Python output)
    a.id as appt_id,
    a.status as appt_status,
    a.appointment_type_text as appt_appointment_type_text,
    a.description as appt_description,
    a.start as appt_start,
    a."end" as appt_end,
    a.minutes_duration as appt_minutes_duration,
    a.created as appt_created,
    a.comment as appt_comment,
    a.patient_instruction as appt_patient_instruction,
    a.cancelation_reason_text as appt_cancelation_reason_text,
    a.priority as appt_priority,

    -- Participant fields (ap_ prefix matching Python output)
    ap.participant_actor_reference as ap_participant_actor_reference,
    ap.participant_actor_type as ap_participant_actor_type,
    ap.participant_required as ap_participant_required,
    ap.participant_status as ap_participant_status,
    ap.participant_period_start as ap_participant_period_start,
    ap.participant_period_end as ap_participant_period_end

FROM fhir_prd_db.appointment a
JOIN fhir_prd_db.appointment_participant ap ON a.id = ap.appointment_id
LEFT JOIN fhir_prd_db.patient_access pa
    ON ap.participant_actor_reference = CONCAT('Patient/', pa.id)
WHERE ap.participant_actor_reference LIKE 'Patient/%'
ORDER BY a.start;