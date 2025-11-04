CREATE OR REPLACE VIEW fhir_prd_db.v_radiation_treatment_appointments AS
WITH
-- ============================================================================
-- Patients who actually have radiation data (from any source)
-- ============================================================================
radiation_patients AS (
    -- Patients with structured ELECT data
    SELECT DISTINCT patient_fhir_id FROM fhir_prd_db.v_radiation_treatments
    UNION
    -- Patients with radiation documents
    SELECT DISTINCT patient_fhir_id FROM fhir_prd_db.v_radiation_documents
    UNION
    -- Patients with radiation care plans
    SELECT DISTINCT patient_fhir_id FROM fhir_prd_db.v_radiation_care_plan_hierarchy
),

-- ============================================================================
-- Appointment service types that indicate radiation oncology
-- ============================================================================
radiation_service_types AS (
    SELECT DISTINCT
        appointment_id,
        service_type_text,
        service_type_coding
    FROM fhir_prd_db.appointment_service_type
    WHERE LOWER(service_type_text) LIKE '%radiation%'
       OR LOWER(service_type_text) LIKE '%rad%onc%'
       OR LOWER(service_type_text) LIKE '%radiotherapy%'
       -- CRITICAL FIX: service_type_coding is JSON stored as VARCHAR
       -- Must search the JSON string for radiation keywords
       OR LOWER(CAST(service_type_coding AS VARCHAR)) LIKE '%rad onc%'
       OR LOWER(CAST(service_type_coding AS VARCHAR)) LIKE '%radiation%'
       OR LOWER(CAST(service_type_coding AS VARCHAR)) LIKE '%radiotherapy%'
),

-- ============================================================================
-- Appointment types that indicate radiation treatment
-- NOTE: Currently returns 0 - this table only contains HL7 v3-ActCode
--       encounter types (AMB/IMP/EMER), not specialty-specific types.
--       Kept as defensive check in case future EHR updates add specialty codes.
-- ============================================================================
radiation_appointment_types AS (
    SELECT DISTINCT
        appointment_id,
        appointment_type_coding_display
    FROM fhir_prd_db.appointment_appointment_type_coding
    WHERE LOWER(appointment_type_coding_display) LIKE '%radiation%'
       OR LOWER(appointment_type_coding_display) LIKE '%rad%onc%'
       OR LOWER(appointment_type_coding_display) LIKE '%radiotherapy%'
)

-- ============================================================================
-- Main query: Return appointments that are ACTUALLY radiation-related
-- ============================================================================
SELECT DISTINCT
    REGEXP_REPLACE(ap.participant_actor_reference, '^Patient/', '') as patient_fhir_id,
    a.id as appointment_id,
    a.status as appointment_status,
    a.appointment_type_text,
    a.priority,
    a.description,
    TRY(CAST(a.start AS TIMESTAMP(3))) as appointment_start,
    TRY(CAST(a."end" AS TIMESTAMP(3))) as appointment_end,
    a.minutes_duration,
    TRY(CAST(a.created AS TIMESTAMP(3))) as created,
    a.comment as appointment_comment,
    a.patient_instruction,

    -- Add provenance: How was this identified as radiation-related?
    CASE
        WHEN rst.appointment_id IS NOT NULL THEN 'service_type_radiation'
        WHEN rat.appointment_id IS NOT NULL THEN 'appointment_type_radiation'
        WHEN rp.patient_fhir_id IS NOT NULL
             AND (LOWER(a.comment) LIKE '%radiation%' OR LOWER(a.description) LIKE '%radiation%')
             THEN 'patient_with_radiation_data_and_radiation_keyword'
        WHEN rp.patient_fhir_id IS NOT NULL THEN 'patient_with_radiation_data_temporal_match'
        ELSE 'unknown'
    END as radiation_identification_method,

    -- Service type details
    rst.service_type_text as radiation_service_type,
    rat.appointment_type_coding_display as radiation_appointment_type

FROM fhir_prd_db.appointment a
JOIN fhir_prd_db.appointment_participant ap ON a.id = ap.appointment_id

-- Join to radiation-specific filters (at least one must match)
LEFT JOIN radiation_service_types rst ON a.id = rst.appointment_id
LEFT JOIN radiation_appointment_types rat ON a.id = rat.appointment_id
LEFT JOIN radiation_patients rp ON REGEXP_REPLACE(ap.participant_actor_reference, '^Patient/', '') = rp.patient_fhir_id

WHERE ap.participant_actor_reference LIKE 'Patient/%'
  AND (
      -- Explicit radiation service type
      rst.appointment_id IS NOT NULL

      -- Explicit radiation appointment type
      OR rat.appointment_id IS NOT NULL

      -- Patient has radiation data AND appointment mentions radiation
      OR (rp.patient_fhir_id IS NOT NULL
          AND (LOWER(a.comment) LIKE '%radiation%'
               OR LOWER(a.comment) LIKE '%rad%onc%'
               OR LOWER(a.comment) LIKE '%radiotherapy%'
               OR LOWER(a.comment) LIKE '%imrt%'
               OR LOWER(a.comment) LIKE '%proton%'
               OR LOWER(a.comment) LIKE '%cyberknife%'
               OR LOWER(a.comment) LIKE '%gamma knife%'
               OR LOWER(a.comment) LIKE '%xrt%'
               OR LOWER(a.description) LIKE '%radiation%'
               OR LOWER(a.description) LIKE '%rad%onc%'
               OR LOWER(a.description) LIKE '%radiotherapy%'
               OR LOWER(a.description) LIKE '%imrt%'
               OR LOWER(a.description) LIKE '%proton%'
               OR LOWER(a.description) LIKE '%cyberknife%'
               OR LOWER(a.description) LIKE '%gamma knife%'
               OR LOWER(a.description) LIKE '%xrt%'
               OR LOWER(a.patient_instruction) LIKE '%radiation%'
               OR LOWER(a.patient_instruction) LIKE '%radiotherapy%'
               OR LOWER(a.patient_instruction) LIKE '%proton%'
               OR LOWER(a.patient_instruction) LIKE '%imrt%'))

      -- Conservative temporal match: Patient has radiation data + appointment during treatment window
      OR (rp.patient_fhir_id IS NOT NULL
          AND a.start IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM fhir_prd_db.v_radiation_treatments vrt
              WHERE vrt.patient_fhir_id = REGEXP_REPLACE(ap.participant_actor_reference, '^Patient/', '')
              AND (
                  -- Within structured treatment dates
                  (vrt.obs_start_date IS NOT NULL
                   AND vrt.obs_stop_date IS NOT NULL
                   AND TRY(CAST(a.start AS TIMESTAMP(3))) >= vrt.obs_start_date
                   AND TRY(CAST(a.start AS TIMESTAMP(3))) <= DATE_ADD('day', 30, vrt.obs_stop_date)) -- 30 day buffer
                  OR
                  -- Within service request treatment dates
                  (vrt.sr_occurrence_period_start IS NOT NULL
                   AND vrt.sr_occurrence_period_end IS NOT NULL
                   AND TRY(CAST(a.start AS TIMESTAMP(3))) >= vrt.sr_occurrence_period_start
                   AND TRY(CAST(a.start AS TIMESTAMP(3))) <= DATE_ADD('day', 30, vrt.sr_occurrence_period_end))
              )
          ))
  )

ORDER BY patient_fhir_id, appointment_start;

-- ============================================================================
-- Patients who actually have radiation data (from any source)
-- ============================================================================
radiation_patients AS (
    -- Patients with structured ELECT data
    SELECT DISTINCT patient_fhir_id FROM fhir_prd_db.v_radiation_treatments
    UNION
    -- Patients with radiation documents
    SELECT DISTINCT patient_fhir_id FROM fhir_prd_db.v_radiation_documents
    UNION
    -- Patients with radiation care plans
    SELECT DISTINCT patient_fhir_id FROM fhir_prd_db.v_radiation_care_plan_hierarchy
),

-- ============================================================================
-- Appointment service types that indicate radiation oncology
-- ============================================================================
radiation_service_types AS (
    SELECT DISTINCT
        appointment_id,
        service_type_text,
        service_type_coding
    FROM fhir_prd_db.appointment_service_type
    WHERE LOWER(service_type_text) LIKE '%radiation%'
       OR LOWER(service_type_text) LIKE '%rad%onc%'
       OR LOWER(service_type_text) LIKE '%radiotherapy%'
       -- CRITICAL FIX: service_type_coding is JSON stored as VARCHAR
       -- Must search the JSON string for radiation keywords
       OR LOWER(CAST(service_type_coding AS VARCHAR)) LIKE '%rad onc%'
       OR LOWER(CAST(service_type_coding AS VARCHAR)) LIKE '%radiation%'
       OR LOWER(CAST(service_type_coding AS VARCHAR)) LIKE '%radiotherapy%'
),

-- ============================================================================
-- Appointment types that indicate radiation treatment
-- NOTE: Currently returns 0 - this table only contains HL7 v3-ActCode
--       encounter types (AMB/IMP/EMER), not specialty-specific types.
--       Kept as defensive check in case future EHR updates add specialty codes.
-- ============================================================================
radiation_appointment_types AS (
    SELECT DISTINCT
        appointment_id,
        appointment_type_coding_display
    FROM fhir_prd_db.appointment_appointment_type_coding
    WHERE LOWER(appointment_type_coding_display) LIKE '%radiation%'
       OR LOWER(appointment_type_coding_display) LIKE '%rad%onc%'
       OR LOWER(appointment_type_coding_display) LIKE '%radiotherapy%'
)

-- ============================================================================
-- Main query: Return appointments that are ACTUALLY radiation-related
-- ============================================================================
SELECT DISTINCT
    REGEXP_REPLACE(ap.participant_actor_reference, '^Patient/', '') as patient_fhir_id,
    a.id as appointment_id,
    a.status as appointment_status,
    a.appointment_type_text,
    a.priority,
    a.description,
    TRY(CAST(a.start AS TIMESTAMP(3))) as appointment_start,
    TRY(CAST(a."end" AS TIMESTAMP(3))) as appointment_end,
    a.minutes_duration,
    TRY(CAST(a.created AS TIMESTAMP(3))) as created,
    a.comment as appointment_comment,
    a.patient_instruction,

    -- Add provenance: How was this identified as radiation-related?
    CASE
        WHEN rst.appointment_id IS NOT NULL THEN 'service_type_radiation'
        WHEN rat.appointment_id IS NOT NULL THEN 'appointment_type_radiation'
        WHEN rp.patient_fhir_id IS NOT NULL
             AND (LOWER(a.comment) LIKE '%radiation%' OR LOWER(a.description) LIKE '%radiation%')
             THEN 'patient_with_radiation_data_and_radiation_keyword'
        WHEN rp.patient_fhir_id IS NOT NULL THEN 'patient_with_radiation_data_temporal_match'
        ELSE 'unknown'
    END as radiation_identification_method,

    -- Service type details
    rst.service_type_text as radiation_service_type,
    rat.appointment_type_coding_display as radiation_appointment_type

FROM fhir_prd_db.appointment a
JOIN fhir_prd_db.appointment_participant ap ON a.id = ap.appointment_id

-- Join to radiation-specific filters (at least one must match)
LEFT JOIN radiation_service_types rst ON a.id = rst.appointment_id
LEFT JOIN radiation_appointment_types rat ON a.id = rat.appointment_id
LEFT JOIN radiation_patients rp ON REGEXP_REPLACE(ap.participant_actor_reference, '^Patient/', '') = rp.patient_fhir_id

WHERE ap.participant_actor_reference LIKE 'Patient/%'
  AND (
      -- Explicit radiation service type
      rst.appointment_id IS NOT NULL

      -- Explicit radiation appointment type
      OR rat.appointment_id IS NOT NULL

      -- Patient has radiation data AND appointment mentions radiation
      OR (rp.patient_fhir_id IS NOT NULL
          AND (LOWER(a.comment) LIKE '%radiation%'
               OR LOWER(a.comment) LIKE '%rad%onc%'
               OR LOWER(a.description) LIKE '%radiation%'
               OR LOWER(a.description) LIKE '%rad%onc%'
               OR LOWER(a.patient_instruction) LIKE '%radiation%'))

      -- Conservative temporal match: Patient has radiation data + appointment during treatment window
      OR (rp.patient_fhir_id IS NOT NULL
          AND a.start IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM fhir_prd_db.v_radiation_treatments vrt
              WHERE vrt.patient_fhir_id = REGEXP_REPLACE(ap.participant_actor_reference, '^Patient/', '')
              AND (
                  -- Within structured treatment dates
                  (vrt.obs_start_date IS NOT NULL
                   AND vrt.obs_stop_date IS NOT NULL
                   AND TRY(CAST(a.start AS TIMESTAMP(3))) >= vrt.obs_start_date
                   AND TRY(CAST(a.start AS TIMESTAMP(3))) <= DATE_ADD('day', 30, vrt.obs_stop_date)) -- 30 day buffer
                  OR
                  -- Within service request treatment dates
                  (vrt.sr_occurrence_period_start IS NOT NULL
                   AND vrt.sr_occurrence_period_end IS NOT NULL
                   AND TRY(CAST(a.start AS TIMESTAMP(3))) >= vrt.sr_occurrence_period_start
                   AND TRY(CAST(a.start AS TIMESTAMP(3))) <= DATE_ADD('day', 30, vrt.sr_occurrence_period_end))
              )
          ))
  )

ORDER BY patient_fhir_id, appointment_start;