CREATE OR REPLACE VIEW fhir_prd_db.v_radiation_treatments AS
WITH
-- ================================================================================
-- CTE 1: Observation-based radiation data (ELECT intake forms)
-- Source: observation + observation_component tables
-- Coverage: ~90 patients with structured dose, site, dates
-- ================================================================================
observation_dose AS (
    SELECT
        o.id as observation_id,
        o.subject_reference as patient_fhir_id,
        oc.component_code_text as course_line,
        CAST(oc.component_value_quantity_value AS DOUBLE) as dose_value,
        COALESCE(oc.component_value_quantity_unit, 'cGy') as dose_unit,
        o.status as observation_status,
        o.effective_date_time as observation_effective_date,
        o.issued as observation_issued_date,
        o.code_text as observation_code_text
    FROM fhir_prd_db.observation o
    JOIN fhir_prd_db.observation_component oc ON o.id = oc.observation_id
    WHERE o.code_text = 'ELECT - INTAKE FORM - RADIATION TABLE - DOSE'
      AND oc.component_value_quantity_value IS NOT NULL
),
observation_field AS (
    SELECT
        o.id as observation_id,
        o.subject_reference as patient_fhir_id,
        oc.component_code_text as course_line,
        oc.component_value_string as field_value,
        o.effective_date_time as observation_effective_date
    FROM fhir_prd_db.observation o
    JOIN fhir_prd_db.observation_component oc ON o.id = oc.observation_id
    WHERE o.code_text = 'ELECT - INTAKE FORM - RADIATION TABLE - FIELD'
      AND oc.component_value_string IS NOT NULL
),
observation_start_date AS (
    SELECT
        o.id as observation_id,
        o.subject_reference as patient_fhir_id,
        oc.component_code_text as course_line,
        oc.component_value_date_time as start_date_value,
        o.issued as observation_issued_date
    FROM fhir_prd_db.observation o
    LEFT JOIN fhir_prd_db.observation_component oc ON o.id = oc.observation_id
    WHERE o.code_text = 'ELECT - INTAKE FORM - RADIATION TABLE - START DATE'
),
observation_stop_date AS (
    SELECT
        o.id as observation_id,
        o.subject_reference as patient_fhir_id,
        oc.component_code_text as course_line,
        oc.component_value_date_time as stop_date_value,
        o.issued as observation_issued_date
    FROM fhir_prd_db.observation o
    LEFT JOIN fhir_prd_db.observation_component oc ON o.id = oc.observation_id
    WHERE o.code_text = 'ELECT - INTAKE FORM - RADIATION TABLE - STOP DATE'
),
observation_comments AS (
    SELECT
        o.id as observation_id,
        o.subject_reference as patient_fhir_id,
        LISTAGG(obn.note_text, ' | ') WITHIN GROUP (ORDER BY obn.note_time) as comments_aggregated,
        LISTAGG(obn.note_author_string, ' | ') WITHIN GROUP (ORDER BY obn.note_time) as comment_authors
    FROM fhir_prd_db.observation o
    LEFT JOIN fhir_prd_db.observation_note obn ON o.id = obn.observation_id
    WHERE o.code_text LIKE 'ELECT - INTAKE FORM - RADIATION TABLE%'
    GROUP BY o.id, o.subject_reference
),
-- Combine all observation components into single record per course
observation_consolidated AS (
    SELECT
        COALESCE(od.patient_fhir_id, of.patient_fhir_id, osd.patient_fhir_id, ost.patient_fhir_id) as patient_fhir_id,
        COALESCE(od.observation_id, of.observation_id, osd.observation_id, ost.observation_id) as observation_id,
        COALESCE(od.course_line, of.course_line, osd.course_line, ost.course_line) as course_line,

        -- Dose fields (obs_ prefix)
        od.dose_value as obs_dose_value,
        od.dose_unit as obs_dose_unit,

        -- Field/site fields (obs_ prefix)
        of.field_value as obs_radiation_field,

        -- Map field to CBTN radiation_site codes (obs_ prefix)
        CASE
            WHEN LOWER(of.field_value) LIKE '%cranial%'
                 AND LOWER(of.field_value) NOT LIKE '%craniospinal%' THEN 1
            WHEN LOWER(of.field_value) LIKE '%craniospinal%' THEN 8
            WHEN LOWER(of.field_value) LIKE '%whole%ventricular%' THEN 9
            WHEN of.field_value IS NOT NULL THEN 6
            ELSE NULL
        END as obs_radiation_site_code,

        -- Dates (obs_ prefix)
        TRY(CAST(osd.start_date_value AS TIMESTAMP(3))) as obs_start_date,
        TRY(CAST(ost.stop_date_value AS TIMESTAMP(3))) as obs_stop_date,

        -- Metadata (obs_ prefix)
        od.observation_status as obs_status,
        TRY(CAST(od.observation_effective_date AS TIMESTAMP(3))) as obs_effective_date,
        TRY(CAST(od.observation_issued_date AS TIMESTAMP(3))) as obs_issued_date,
        od.observation_code_text as obs_code_text,

        -- Comments (obsc_ prefix for component-level data)
        oc.comments_aggregated as obsc_comments,
        oc.comment_authors as obsc_comment_authors,

        -- Data source flag
        'observation' as data_source_primary

    FROM observation_dose od
    FULL OUTER JOIN observation_field of
        ON od.patient_fhir_id = of.patient_fhir_id
        AND od.course_line = of.course_line
    FULL OUTER JOIN observation_start_date osd
        ON COALESCE(od.patient_fhir_id, of.patient_fhir_id) = osd.patient_fhir_id
        AND COALESCE(od.course_line, of.course_line) = osd.course_line
    FULL OUTER JOIN observation_stop_date ost
        ON COALESCE(od.patient_fhir_id, of.patient_fhir_id, osd.patient_fhir_id) = ost.patient_fhir_id
        AND COALESCE(od.course_line, of.course_line, osd.course_line) = ost.course_line
    LEFT JOIN observation_comments oc
        ON COALESCE(od.observation_id, of.observation_id, osd.observation_id, ost.observation_id) = oc.observation_id
),

-- ================================================================================
-- CTE 2: Service Request radiation courses
-- Source: service_request table
-- Coverage: 3 records, 1 patient (very limited)
-- ================================================================================
service_request_courses AS (
    SELECT
        sr.subject_reference as patient_fhir_id,
        sr.id as service_request_id,

        -- Service request fields (sr_ prefix - PRESERVE ALL ORIGINAL FIELDS)
        sr.status as sr_status,
        sr.intent as sr_intent,
        sr.code_text as sr_code_text,
        sr.quantity_quantity_value as sr_quantity_value,
        sr.quantity_quantity_unit as sr_quantity_unit,
        sr.quantity_ratio_numerator_value as sr_quantity_ratio_numerator_value,
        sr.quantity_ratio_numerator_unit as sr_quantity_ratio_numerator_unit,
        sr.quantity_ratio_denominator_value as sr_quantity_ratio_denominator_value,
        sr.quantity_ratio_denominator_unit as sr_quantity_ratio_denominator_unit,
        TRY(CAST(sr.occurrence_date_time AS TIMESTAMP(3))) as sr_occurrence_date_time,
        TRY(CAST(sr.occurrence_period_start AS TIMESTAMP(3))) as sr_occurrence_period_start,
        TRY(CAST(sr.occurrence_period_end AS TIMESTAMP(3))) as sr_occurrence_period_end,
        TRY(CAST(sr.authored_on AS TIMESTAMP(3))) as sr_authored_on,
        sr.requester_reference as sr_requester_reference,
        sr.requester_display as sr_requester_display,
        sr.performer_type_text as sr_performer_type_text,
        sr.patient_instruction as sr_patient_instruction,
        sr.priority as sr_priority,
        sr.do_not_perform as sr_do_not_perform,

        -- Data source flag
        'service_request' as data_source_primary

    FROM fhir_prd_db.service_request sr
    WHERE sr.subject_reference IS NOT NULL
      AND (LOWER(sr.code_text) LIKE '%radiation%'
           OR LOWER(sr.code_text) LIKE '%radiotherapy%'
           OR LOWER(sr.code_text) LIKE '%imrt%'
           OR LOWER(sr.code_text) LIKE '%proton%'
           OR LOWER(sr.code_text) LIKE '%cyberknife%'
           OR LOWER(sr.code_text) LIKE '%cyber knife%'
           OR LOWER(sr.code_text) LIKE '%gamma knife%'
           OR LOWER(sr.code_text) LIKE '%gammaknife%'
           OR LOWER(sr.code_text) LIKE '%xrt%'
           OR LOWER(sr.code_text) LIKE '%x-rt%'
           OR LOWER(sr.patient_instruction) LIKE '%radiation%'
           OR LOWER(sr.patient_instruction) LIKE '%radiotherapy%'
           OR LOWER(sr.patient_instruction) LIKE '%imrt%'
           OR LOWER(sr.patient_instruction) LIKE '%proton%'
           OR LOWER(sr.patient_instruction) LIKE '%cyberknife%'
           OR LOWER(sr.patient_instruction) LIKE '%gamma knife%')
),

-- ================================================================================
-- CTE 3: Service Request sub-schemas (notes, reason codes, body sites)
-- Source: service_request_* tables
-- ================================================================================
service_request_notes AS (
    SELECT
        srn.service_request_id,
        LISTAGG(srn.note_text, ' | ') WITHIN GROUP (ORDER BY srn.note_time) as note_text_aggregated,
        LISTAGG(srn.note_author_reference_display, ' | ') WITHIN GROUP (ORDER BY srn.note_time) as note_authors
    FROM fhir_prd_db.service_request_note srn
    WHERE srn.service_request_id IN (
        SELECT id FROM fhir_prd_db.service_request sr
        WHERE LOWER(sr.code_text) LIKE '%radiation%'
           OR LOWER(sr.code_text) LIKE '%radiotherapy%'
           OR LOWER(sr.code_text) LIKE '%imrt%'
           OR LOWER(sr.code_text) LIKE '%proton%'
           OR LOWER(sr.code_text) LIKE '%cyberknife%'
           OR LOWER(sr.code_text) LIKE '%gamma knife%'
           OR LOWER(sr.code_text) LIKE '%xrt%'
           OR LOWER(sr.patient_instruction) LIKE '%radiation%'
           OR LOWER(sr.patient_instruction) LIKE '%radiotherapy%'
           OR LOWER(sr.patient_instruction) LIKE '%imrt%'
           OR LOWER(sr.patient_instruction) LIKE '%proton%'
           OR LOWER(sr.patient_instruction) LIKE '%cyberknife%'
           OR LOWER(sr.patient_instruction) LIKE '%gamma knife%'
    )
    GROUP BY srn.service_request_id
),
service_request_reason_codes AS (
    SELECT
        srrc.service_request_id,
        LISTAGG(srrc.reason_code_text, ' | ') WITHIN GROUP (ORDER BY srrc.reason_code_text) as reason_code_text_aggregated,
        LISTAGG(srrc.reason_code_coding, ' | ') WITHIN GROUP (ORDER BY srrc.reason_code_text) as reason_code_coding_aggregated
    FROM fhir_prd_db.service_request_reason_code srrc
    WHERE srrc.service_request_id IN (
        SELECT id FROM fhir_prd_db.service_request sr
        WHERE LOWER(sr.code_text) LIKE '%radiation%'
           OR LOWER(sr.code_text) LIKE '%radiotherapy%'
           OR LOWER(sr.code_text) LIKE '%imrt%'
           OR LOWER(sr.code_text) LIKE '%proton%'
           OR LOWER(sr.code_text) LIKE '%cyberknife%'
           OR LOWER(sr.code_text) LIKE '%gamma knife%'
           OR LOWER(sr.code_text) LIKE '%xrt%'
           OR LOWER(sr.patient_instruction) LIKE '%radiation%'
           OR LOWER(sr.patient_instruction) LIKE '%radiotherapy%'
           OR LOWER(sr.patient_instruction) LIKE '%imrt%'
           OR LOWER(sr.patient_instruction) LIKE '%proton%'
           OR LOWER(sr.patient_instruction) LIKE '%cyberknife%'
           OR LOWER(sr.patient_instruction) LIKE '%gamma knife%'
    )
    GROUP BY srrc.service_request_id
),
service_request_body_sites AS (
    SELECT
        srbs.service_request_id,
        LISTAGG(srbs.body_site_text, ' | ') WITHIN GROUP (ORDER BY srbs.body_site_text) as body_site_text_aggregated,
        LISTAGG(srbs.body_site_coding, ' | ') WITHIN GROUP (ORDER BY srbs.body_site_text) as body_site_coding_aggregated
    FROM fhir_prd_db.service_request_body_site srbs
    WHERE srbs.service_request_id IN (
        SELECT id FROM fhir_prd_db.service_request sr
        WHERE LOWER(sr.code_text) LIKE '%radiation%'
           OR LOWER(sr.code_text) LIKE '%radiotherapy%'
           OR LOWER(sr.code_text) LIKE '%imrt%'
           OR LOWER(sr.code_text) LIKE '%proton%'
           OR LOWER(sr.code_text) LIKE '%cyberknife%'
           OR LOWER(sr.code_text) LIKE '%gamma knife%'
           OR LOWER(sr.code_text) LIKE '%xrt%'
           OR LOWER(sr.patient_instruction) LIKE '%radiation%'
           OR LOWER(sr.patient_instruction) LIKE '%radiotherapy%'
           OR LOWER(sr.patient_instruction) LIKE '%imrt%'
           OR LOWER(sr.patient_instruction) LIKE '%proton%'
           OR LOWER(sr.patient_instruction) LIKE '%cyberknife%'
           OR LOWER(sr.patient_instruction) LIKE '%gamma knife%'
    )
    GROUP BY srbs.service_request_id
),

-- ================================================================================
-- CTE 4: Radiation patients list (for appointment filtering)
-- Source: service_request + observation tables
-- Purpose: Pre-materialize patient list for performance
-- ================================================================================
radiation_patients_list AS (
    SELECT DISTINCT subject_reference as patient_id FROM fhir_prd_db.service_request
    WHERE LOWER(code_text) LIKE '%radiation%'
       OR LOWER(code_text) LIKE '%radiotherapy%'
       OR LOWER(code_text) LIKE '%imrt%'
       OR LOWER(code_text) LIKE '%proton%'
       OR LOWER(code_text) LIKE '%cyberknife%'
       OR LOWER(code_text) LIKE '%gamma knife%'
       OR LOWER(code_text) LIKE '%xrt%'
    UNION
    SELECT DISTINCT subject_reference as patient_id FROM fhir_prd_db.observation
    WHERE code_text LIKE 'ELECT - INTAKE FORM - RADIATION%'
),

-- ================================================================================
-- CTE 5: Appointment data (scheduling context)
-- Source: appointment + appointment_participant tables
-- Coverage: 331,796 appointments, 1,855 patients
-- ================================================================================
appointment_summary AS (
    SELECT
        REGEXP_REPLACE(ap.participant_actor_reference, '^Patient/', '') as patient_fhir_id,
        COUNT(DISTINCT a.id) as total_appointments,
        COUNT(DISTINCT CASE WHEN a.status = 'fulfilled' THEN a.id END) as fulfilled_appointments,
        COUNT(DISTINCT CASE WHEN a.status = 'cancelled' THEN a.id END) as cancelled_appointments,
        COUNT(DISTINCT CASE WHEN a.status = 'noshow' THEN a.id END) as noshow_appointments,
        TRY(CAST(MIN(a.start) AS TIMESTAMP(3))) as first_appointment_date,
        TRY(CAST(MAX(a.start) AS TIMESTAMP(3))) as last_appointment_date,
        TRY(CAST(MIN(CASE WHEN a.status = 'fulfilled' THEN a.start END) AS TIMESTAMP(3))) as first_fulfilled_appointment,
        TRY(CAST(MAX(CASE WHEN a.status = 'fulfilled' THEN a.start END) AS TIMESTAMP(3))) as last_fulfilled_appointment
    FROM fhir_prd_db.appointment a
    JOIN fhir_prd_db.appointment_participant ap ON a.id = ap.appointment_id
    WHERE ap.participant_actor_reference LIKE 'Patient/%'
      AND REGEXP_REPLACE(ap.participant_actor_reference, '^Patient/', '') IN (SELECT patient_id FROM radiation_patients_list)
    GROUP BY REGEXP_REPLACE(ap.participant_actor_reference, '^Patient/', '')
),

-- ================================================================================
-- CTE 6: Care Plan data (treatment plan context)
-- Source: care_plan + care_plan_part_of tables
-- Coverage: 18,189 records, 568 patients
-- ================================================================================
care_plan_summary AS (
    SELECT
        cp.subject_reference as patient_fhir_id,
        COUNT(DISTINCT cp.id) as total_care_plans,
        LISTAGG(DISTINCT cp.title, ' | ') WITHIN GROUP (ORDER BY cp.title) as care_plan_titles,
        LISTAGG(DISTINCT cp.status, ' | ') WITHIN GROUP (ORDER BY cp.status) as care_plan_statuses,
        TRY(CAST(MIN(cp.period_start) AS TIMESTAMP(3))) as first_care_plan_start,
        TRY(CAST(MAX(cp.period_end) AS TIMESTAMP(3))) as last_care_plan_end
    FROM fhir_prd_db.care_plan cp
    WHERE cp.subject_reference IS NOT NULL
      AND (LOWER(cp.title) LIKE '%radiation%')
    GROUP BY cp.subject_reference
)

-- ================================================================================
-- MAIN SELECT: Combine all sources with field provenance
-- ================================================================================
SELECT
    -- Patient identifier
    COALESCE(oc.patient_fhir_id, src.patient_fhir_id, apt.patient_fhir_id, cps.patient_fhir_id) as patient_fhir_id,

    -- Primary data source indicator
    COALESCE(oc.data_source_primary, src.data_source_primary) as data_source_primary,

    -- Course identifier (composite key)
    COALESCE(oc.observation_id, src.service_request_id) as course_id,
    oc.course_line as obs_course_line_number,

    -- ============================================================================
    -- OBSERVATION FIELDS (obs_ prefix) - STRUCTURED DOSE/SITE DATA
    -- Source: observation + observation_component tables (ELECT intake forms)
    -- ============================================================================
    oc.obs_dose_value,
    oc.obs_dose_unit,
    oc.obs_radiation_field,
    oc.obs_radiation_site_code,
    oc.obs_start_date,
    oc.obs_stop_date,
    oc.obs_status,
    oc.obs_effective_date,
    oc.obs_issued_date,
    oc.obs_code_text,

    -- Observation component comments (obsc_ prefix)
    oc.obsc_comments,
    oc.obsc_comment_authors,

    -- ============================================================================
    -- SERVICE REQUEST FIELDS (sr_ prefix) - TREATMENT COURSE METADATA
    -- Source: service_request table
    -- ============================================================================
    src.sr_status,
    src.sr_intent,
    src.sr_code_text,
    src.sr_quantity_value,
    src.sr_quantity_unit,
    src.sr_quantity_ratio_numerator_value,
    src.sr_quantity_ratio_numerator_unit,
    src.sr_quantity_ratio_denominator_value,
    src.sr_quantity_ratio_denominator_unit,
    src.sr_occurrence_date_time,
    src.sr_occurrence_period_start,
    src.sr_occurrence_period_end,
    src.sr_authored_on,
    src.sr_requester_reference,
    src.sr_requester_display,
    src.sr_performer_type_text,
    src.sr_patient_instruction,
    src.sr_priority,
    src.sr_do_not_perform,

    -- Service request sub-schema fields (srn_, srrc_, srbs_ prefixes)
    srn.note_text_aggregated as srn_note_text,
    srn.note_authors as srn_note_authors,
    srrc.reason_code_text_aggregated as srrc_reason_code_text,
    srrc.reason_code_coding_aggregated as srrc_reason_code_coding,
    srbs.body_site_text_aggregated as srbs_body_site_text,
    srbs.body_site_coding_aggregated as srbs_body_site_coding,

    -- ============================================================================
    -- APPOINTMENT FIELDS (apt_ prefix) - SCHEDULING CONTEXT
    -- Source: appointment + appointment_participant tables
    -- ============================================================================
    apt.total_appointments as apt_total_appointments,
    apt.fulfilled_appointments as apt_fulfilled_appointments,
    apt.cancelled_appointments as apt_cancelled_appointments,
    apt.noshow_appointments as apt_noshow_appointments,
    apt.first_appointment_date as apt_first_appointment_date,
    apt.last_appointment_date as apt_last_appointment_date,
    apt.first_fulfilled_appointment as apt_first_fulfilled_appointment,
    apt.last_fulfilled_appointment as apt_last_fulfilled_appointment,

    -- ============================================================================
    -- CARE PLAN FIELDS (cp_ prefix) - TREATMENT PLAN CONTEXT
    -- Source: care_plan + care_plan_part_of tables
    -- ============================================================================
    cps.total_care_plans as cp_total_care_plans,
    cps.care_plan_titles as cp_titles,
    cps.care_plan_statuses as cp_statuses,
    cps.first_care_plan_start as cp_first_start_date,
    cps.last_care_plan_end as cp_last_end_date,

    -- ============================================================================
    -- DERIVED/COMPUTED FIELDS
    -- ============================================================================

    -- Best available treatment dates (prioritize observation over service_request)
    COALESCE(oc.obs_start_date, src.sr_occurrence_period_start, apt.first_fulfilled_appointment) as best_treatment_start_date,
    COALESCE(oc.obs_stop_date, src.sr_occurrence_period_end, apt.last_fulfilled_appointment) as best_treatment_stop_date,

    -- Data completeness indicators
    CASE WHEN oc.obs_dose_value IS NOT NULL THEN true ELSE false END as has_structured_dose,
    CASE WHEN oc.obs_radiation_field IS NOT NULL THEN true ELSE false END as has_structured_site,
    CASE WHEN oc.obs_start_date IS NOT NULL OR src.sr_occurrence_period_start IS NOT NULL THEN true ELSE false END as has_treatment_dates,
    CASE WHEN apt.total_appointments > 0 THEN true ELSE false END as has_appointments,
    CASE WHEN cps.total_care_plans > 0 THEN true ELSE false END as has_care_plan,

    -- Data quality score (0-1)
    (
        CAST(CASE WHEN oc.obs_dose_value IS NOT NULL THEN 1 ELSE 0 END AS DOUBLE) * 0.3 +
        CAST(CASE WHEN oc.obs_radiation_field IS NOT NULL THEN 1 ELSE 0 END AS DOUBLE) * 0.3 +
        CAST(CASE WHEN oc.obs_start_date IS NOT NULL OR src.sr_occurrence_period_start IS NOT NULL THEN 1 ELSE 0 END AS DOUBLE) * 0.2 +
        CAST(CASE WHEN apt.total_appointments > 0 THEN 1 ELSE 0 END AS DOUBLE) * 0.1 +
        CAST(CASE WHEN cps.total_care_plans > 0 THEN 1 ELSE 0 END AS DOUBLE) * 0.1
    ) as data_quality_score

FROM observation_consolidated oc
FULL OUTER JOIN service_request_courses src
    ON oc.patient_fhir_id = src.patient_fhir_id
LEFT JOIN service_request_notes srn ON src.service_request_id = srn.service_request_id
LEFT JOIN service_request_reason_codes srrc ON src.service_request_id = srrc.service_request_id
LEFT JOIN service_request_body_sites srbs ON src.service_request_id = srbs.service_request_id
LEFT JOIN appointment_summary apt
    ON COALESCE(oc.patient_fhir_id, src.patient_fhir_id) = apt.patient_fhir_id
LEFT JOIN care_plan_summary cps
    ON COALESCE(oc.patient_fhir_id, src.patient_fhir_id) = cps.patient_fhir_id
WHERE COALESCE(oc.patient_fhir_id, src.patient_fhir_id, apt.patient_fhir_id, cps.patient_fhir_id) IS NOT NULL

ORDER BY patient_fhir_id, obs_course_line_number, best_treatment_start_date;

-- ================================================================================
-- CTE 1: Observation-based radiation data (ELECT intake forms)
-- Source: observation + observation_component tables
-- Coverage: ~90 patients with structured dose, site, dates
-- ================================================================================
observation_dose AS (
    SELECT
        o.id as observation_id,
        o.subject_reference as patient_fhir_id,
        oc.component_code_text as course_line,
        CAST(oc.component_value_quantity_value AS DOUBLE) as dose_value,
        COALESCE(oc.component_value_quantity_unit, 'cGy') as dose_unit,
        o.status as observation_status,
        o.effective_date_time as observation_effective_date,
        o.issued as observation_issued_date,
        o.code_text as observation_code_text
    FROM fhir_prd_db.observation o
    JOIN fhir_prd_db.observation_component oc ON o.id = oc.observation_id
    WHERE o.code_text = 'ELECT - INTAKE FORM - RADIATION TABLE - DOSE'
      AND oc.component_value_quantity_value IS NOT NULL
),
observation_field AS (
    SELECT
        o.id as observation_id,
        o.subject_reference as patient_fhir_id,
        oc.component_code_text as course_line,
        oc.component_value_string as field_value,
        o.effective_date_time as observation_effective_date
    FROM fhir_prd_db.observation o
    JOIN fhir_prd_db.observation_component oc ON o.id = oc.observation_id
    WHERE o.code_text = 'ELECT - INTAKE FORM - RADIATION TABLE - FIELD'
      AND oc.component_value_string IS NOT NULL
),
observation_start_date AS (
    SELECT
        o.id as observation_id,
        o.subject_reference as patient_fhir_id,
        oc.component_code_text as course_line,
        oc.component_value_date_time as start_date_value,
        o.issued as observation_issued_date
    FROM fhir_prd_db.observation o
    LEFT JOIN fhir_prd_db.observation_component oc ON o.id = oc.observation_id
    WHERE o.code_text = 'ELECT - INTAKE FORM - RADIATION TABLE - START DATE'
),
observation_stop_date AS (
    SELECT
        o.id as observation_id,
        o.subject_reference as patient_fhir_id,
        oc.component_code_text as course_line,
        oc.component_value_date_time as stop_date_value,
        o.issued as observation_issued_date
    FROM fhir_prd_db.observation o
    LEFT JOIN fhir_prd_db.observation_component oc ON o.id = oc.observation_id
    WHERE o.code_text = 'ELECT - INTAKE FORM - RADIATION TABLE - STOP DATE'
),
observation_comments AS (
    SELECT
        o.id as observation_id,
        o.subject_reference as patient_fhir_id,
        LISTAGG(obn.note_text, ' | ') WITHIN GROUP (ORDER BY obn.note_time) as comments_aggregated,
        LISTAGG(obn.note_author_string, ' | ') WITHIN GROUP (ORDER BY obn.note_time) as comment_authors
    FROM fhir_prd_db.observation o
    LEFT JOIN fhir_prd_db.observation_note obn ON o.id = obn.observation_id
    WHERE o.code_text LIKE 'ELECT - INTAKE FORM - RADIATION TABLE%'
    GROUP BY o.id, o.subject_reference
),
-- Combine all observation components into single record per course
observation_consolidated AS (
    SELECT
        COALESCE(od.patient_fhir_id, of.patient_fhir_id, osd.patient_fhir_id, ost.patient_fhir_id) as patient_fhir_id,
        COALESCE(od.observation_id, of.observation_id, osd.observation_id, ost.observation_id) as observation_id,
        COALESCE(od.course_line, of.course_line, osd.course_line, ost.course_line) as course_line,

        -- Dose fields (obs_ prefix)
        od.dose_value as obs_dose_value,
        od.dose_unit as obs_dose_unit,

        -- Field/site fields (obs_ prefix)
        of.field_value as obs_radiation_field,

        -- Map field to CBTN radiation_site codes (obs_ prefix)
        CASE
            WHEN LOWER(of.field_value) LIKE '%cranial%'
                 AND LOWER(of.field_value) NOT LIKE '%craniospinal%' THEN 1
            WHEN LOWER(of.field_value) LIKE '%craniospinal%' THEN 8
            WHEN LOWER(of.field_value) LIKE '%whole%ventricular%' THEN 9
            WHEN of.field_value IS NOT NULL THEN 6
            ELSE NULL
        END as obs_radiation_site_code,

        -- Dates (obs_ prefix)
        TRY(CAST(osd.start_date_value AS TIMESTAMP(3))) as obs_start_date,
        TRY(CAST(ost.stop_date_value AS TIMESTAMP(3))) as obs_stop_date,

        -- Metadata (obs_ prefix)
        od.observation_status as obs_status,
        TRY(CAST(od.observation_effective_date AS TIMESTAMP(3))) as obs_effective_date,
        TRY(CAST(od.observation_issued_date AS TIMESTAMP(3))) as obs_issued_date,
        od.observation_code_text as obs_code_text,

        -- Comments (obsc_ prefix for component-level data)
        oc.comments_aggregated as obsc_comments,
        oc.comment_authors as obsc_comment_authors,

        -- Data source flag
        'observation' as data_source_primary

    FROM observation_dose od
    FULL OUTER JOIN observation_field of
        ON od.patient_fhir_id = of.patient_fhir_id
        AND od.course_line = of.course_line
    FULL OUTER JOIN observation_start_date osd
        ON COALESCE(od.patient_fhir_id, of.patient_fhir_id) = osd.patient_fhir_id
        AND COALESCE(od.course_line, of.course_line) = osd.course_line
    FULL OUTER JOIN observation_stop_date ost
        ON COALESCE(od.patient_fhir_id, of.patient_fhir_id, osd.patient_fhir_id) = ost.patient_fhir_id
        AND COALESCE(od.course_line, of.course_line, osd.course_line) = ost.course_line
    LEFT JOIN observation_comments oc
        ON COALESCE(od.observation_id, of.observation_id, osd.observation_id, ost.observation_id) = oc.observation_id
),

-- ================================================================================
-- CTE 2: Service Request radiation courses
-- Source: service_request table
-- Coverage: 3 records, 1 patient (very limited)
-- ================================================================================
service_request_courses AS (
    SELECT
        sr.subject_reference as patient_fhir_id,
        sr.id as service_request_id,

        -- Service request fields (sr_ prefix - PRESERVE ALL ORIGINAL FIELDS)
        sr.status as sr_status,
        sr.intent as sr_intent,
        sr.code_text as sr_code_text,
        sr.quantity_quantity_value as sr_quantity_value,
        sr.quantity_quantity_unit as sr_quantity_unit,
        sr.quantity_ratio_numerator_value as sr_quantity_ratio_numerator_value,
        sr.quantity_ratio_numerator_unit as sr_quantity_ratio_numerator_unit,
        sr.quantity_ratio_denominator_value as sr_quantity_ratio_denominator_value,
        sr.quantity_ratio_denominator_unit as sr_quantity_ratio_denominator_unit,
        TRY(CAST(sr.occurrence_date_time AS TIMESTAMP(3))) as sr_occurrence_date_time,
        TRY(CAST(sr.occurrence_period_start AS TIMESTAMP(3))) as sr_occurrence_period_start,
        TRY(CAST(sr.occurrence_period_end AS TIMESTAMP(3))) as sr_occurrence_period_end,
        TRY(CAST(sr.authored_on AS TIMESTAMP(3))) as sr_authored_on,
        sr.requester_reference as sr_requester_reference,
        sr.requester_display as sr_requester_display,
        sr.performer_type_text as sr_performer_type_text,
        sr.patient_instruction as sr_patient_instruction,
        sr.priority as sr_priority,
        sr.do_not_perform as sr_do_not_perform,

        -- Data source flag
        'service_request' as data_source_primary

    FROM fhir_prd_db.service_request sr
    WHERE sr.subject_reference IS NOT NULL
      AND (LOWER(sr.code_text) LIKE '%radiation%'
           OR LOWER(sr.patient_instruction) LIKE '%radiation%')
),

-- ================================================================================
-- CTE 3: Service Request sub-schemas (notes, reason codes, body sites)
-- Source: service_request_* tables
-- ================================================================================
service_request_notes AS (
    SELECT
        srn.service_request_id,
        LISTAGG(srn.note_text, ' | ') WITHIN GROUP (ORDER BY srn.note_time) as note_text_aggregated,
        LISTAGG(srn.note_author_reference_display, ' | ') WITHIN GROUP (ORDER BY srn.note_time) as note_authors
    FROM fhir_prd_db.service_request_note srn
    WHERE srn.service_request_id IN (
        SELECT id FROM fhir_prd_db.service_request sr
        WHERE LOWER(sr.code_text) LIKE '%radiation%' OR LOWER(sr.patient_instruction) LIKE '%radiation%'
    )
    GROUP BY srn.service_request_id
),
service_request_reason_codes AS (
    SELECT
        srrc.service_request_id,
        LISTAGG(srrc.reason_code_text, ' | ') WITHIN GROUP (ORDER BY srrc.reason_code_text) as reason_code_text_aggregated,
        LISTAGG(srrc.reason_code_coding, ' | ') WITHIN GROUP (ORDER BY srrc.reason_code_text) as reason_code_coding_aggregated
    FROM fhir_prd_db.service_request_reason_code srrc
    WHERE srrc.service_request_id IN (
        SELECT id FROM fhir_prd_db.service_request sr
        WHERE LOWER(sr.code_text) LIKE '%radiation%' OR LOWER(sr.patient_instruction) LIKE '%radiation%'
    )
    GROUP BY srrc.service_request_id
),
service_request_body_sites AS (
    SELECT
        srbs.service_request_id,
        LISTAGG(srbs.body_site_text, ' | ') WITHIN GROUP (ORDER BY srbs.body_site_text) as body_site_text_aggregated,
        LISTAGG(srbs.body_site_coding, ' | ') WITHIN GROUP (ORDER BY srbs.body_site_text) as body_site_coding_aggregated
    FROM fhir_prd_db.service_request_body_site srbs
    WHERE srbs.service_request_id IN (
        SELECT id FROM fhir_prd_db.service_request sr
        WHERE LOWER(sr.code_text) LIKE '%radiation%' OR LOWER(sr.patient_instruction) LIKE '%radiation%'
    )
    GROUP BY srbs.service_request_id
),

-- ================================================================================
-- CTE 4: Radiation patients list (for appointment filtering)
-- Source: service_request + observation tables
-- Purpose: Pre-materialize patient list for performance
-- ================================================================================
radiation_patients_list AS (
    SELECT DISTINCT subject_reference as patient_id FROM fhir_prd_db.service_request
    WHERE LOWER(code_text) LIKE '%radiation%'
    UNION
    SELECT DISTINCT subject_reference as patient_id FROM fhir_prd_db.observation
    WHERE code_text LIKE 'ELECT - INTAKE FORM - RADIATION%'
),

-- ================================================================================
-- CTE 5: Appointment data (scheduling context)
-- Source: appointment + appointment_participant tables
-- Coverage: 331,796 appointments, 1,855 patients
-- ================================================================================
appointment_summary AS (
    SELECT
        REGEXP_REPLACE(ap.participant_actor_reference, '^Patient/', '') as patient_fhir_id,
        COUNT(DISTINCT a.id) as total_appointments,
        COUNT(DISTINCT CASE WHEN a.status = 'fulfilled' THEN a.id END) as fulfilled_appointments,
        COUNT(DISTINCT CASE WHEN a.status = 'cancelled' THEN a.id END) as cancelled_appointments,
        COUNT(DISTINCT CASE WHEN a.status = 'noshow' THEN a.id END) as noshow_appointments,
        TRY(CAST(MIN(a.start) AS TIMESTAMP(3))) as first_appointment_date,
        TRY(CAST(MAX(a.start) AS TIMESTAMP(3))) as last_appointment_date,
        TRY(CAST(MIN(CASE WHEN a.status = 'fulfilled' THEN a.start END) AS TIMESTAMP(3))) as first_fulfilled_appointment,
        TRY(CAST(MAX(CASE WHEN a.status = 'fulfilled' THEN a.start END) AS TIMESTAMP(3))) as last_fulfilled_appointment
    FROM fhir_prd_db.appointment a
    JOIN fhir_prd_db.appointment_participant ap ON a.id = ap.appointment_id
    WHERE ap.participant_actor_reference LIKE 'Patient/%'
      AND REGEXP_REPLACE(ap.participant_actor_reference, '^Patient/', '') IN (SELECT patient_id FROM radiation_patients_list)
    GROUP BY REGEXP_REPLACE(ap.participant_actor_reference, '^Patient/', '')
),

-- ================================================================================
-- CTE 6: Care Plan data (treatment plan context)
-- Source: care_plan + care_plan_part_of tables
-- Coverage: 18,189 records, 568 patients
-- ================================================================================
care_plan_summary AS (
    SELECT
        cp.subject_reference as patient_fhir_id,
        COUNT(DISTINCT cp.id) as total_care_plans,
        LISTAGG(DISTINCT cp.title, ' | ') WITHIN GROUP (ORDER BY cp.title) as care_plan_titles,
        LISTAGG(DISTINCT cp.status, ' | ') WITHIN GROUP (ORDER BY cp.status) as care_plan_statuses,
        TRY(CAST(MIN(cp.period_start) AS TIMESTAMP(3))) as first_care_plan_start,
        TRY(CAST(MAX(cp.period_end) AS TIMESTAMP(3))) as last_care_plan_end
    FROM fhir_prd_db.care_plan cp
    WHERE cp.subject_reference IS NOT NULL
      AND (LOWER(cp.title) LIKE '%radiation%')
    GROUP BY cp.subject_reference
)

-- ================================================================================
-- MAIN SELECT: Combine all sources with field provenance
-- ================================================================================
SELECT
    -- Patient identifier
    COALESCE(oc.patient_fhir_id, src.patient_fhir_id, apt.patient_fhir_id, cps.patient_fhir_id) as patient_fhir_id,

    -- Primary data source indicator
    COALESCE(oc.data_source_primary, src.data_source_primary) as data_source_primary,

    -- Course identifier (composite key)
    COALESCE(oc.observation_id, src.service_request_id) as course_id,
    oc.course_line as obs_course_line_number,

    -- ============================================================================
    -- OBSERVATION FIELDS (obs_ prefix) - STRUCTURED DOSE/SITE DATA
    -- Source: observation + observation_component tables (ELECT intake forms)
    -- ============================================================================
    oc.obs_dose_value,
    oc.obs_dose_unit,
    oc.obs_radiation_field,
    oc.obs_radiation_site_code,
    oc.obs_start_date,
    oc.obs_stop_date,
    oc.obs_status,
    oc.obs_effective_date,
    oc.obs_issued_date,
    oc.obs_code_text,

    -- Observation component comments (obsc_ prefix)
    oc.obsc_comments,
    oc.obsc_comment_authors,

    -- ============================================================================
    -- SERVICE REQUEST FIELDS (sr_ prefix) - TREATMENT COURSE METADATA
    -- Source: service_request table
    -- ============================================================================
    src.sr_status,
    src.sr_intent,
    src.sr_code_text,
    src.sr_quantity_value,
    src.sr_quantity_unit,
    src.sr_quantity_ratio_numerator_value,
    src.sr_quantity_ratio_numerator_unit,
    src.sr_quantity_ratio_denominator_value,
    src.sr_quantity_ratio_denominator_unit,
    src.sr_occurrence_date_time,
    src.sr_occurrence_period_start,
    src.sr_occurrence_period_end,
    src.sr_authored_on,
    src.sr_requester_reference,
    src.sr_requester_display,
    src.sr_performer_type_text,
    src.sr_patient_instruction,
    src.sr_priority,
    src.sr_do_not_perform,

    -- Service request sub-schema fields (srn_, srrc_, srbs_ prefixes)
    srn.note_text_aggregated as srn_note_text,
    srn.note_authors as srn_note_authors,
    srrc.reason_code_text_aggregated as srrc_reason_code_text,
    srrc.reason_code_coding_aggregated as srrc_reason_code_coding,
    srbs.body_site_text_aggregated as srbs_body_site_text,
    srbs.body_site_coding_aggregated as srbs_body_site_coding,

    -- ============================================================================
    -- APPOINTMENT FIELDS (apt_ prefix) - SCHEDULING CONTEXT
    -- Source: appointment + appointment_participant tables
    -- ============================================================================
    apt.total_appointments as apt_total_appointments,
    apt.fulfilled_appointments as apt_fulfilled_appointments,
    apt.cancelled_appointments as apt_cancelled_appointments,
    apt.noshow_appointments as apt_noshow_appointments,
    apt.first_appointment_date as apt_first_appointment_date,
    apt.last_appointment_date as apt_last_appointment_date,
    apt.first_fulfilled_appointment as apt_first_fulfilled_appointment,
    apt.last_fulfilled_appointment as apt_last_fulfilled_appointment,

    -- ============================================================================
    -- CARE PLAN FIELDS (cp_ prefix) - TREATMENT PLAN CONTEXT
    -- Source: care_plan + care_plan_part_of tables
    -- ============================================================================
    cps.total_care_plans as cp_total_care_plans,
    cps.care_plan_titles as cp_titles,
    cps.care_plan_statuses as cp_statuses,
    cps.first_care_plan_start as cp_first_start_date,
    cps.last_care_plan_end as cp_last_end_date,

    -- ============================================================================
    -- DERIVED/COMPUTED FIELDS
    -- ============================================================================

    -- Best available treatment dates (prioritize observation over service_request)
    COALESCE(oc.obs_start_date, src.sr_occurrence_period_start, apt.first_fulfilled_appointment) as best_treatment_start_date,
    COALESCE(oc.obs_stop_date, src.sr_occurrence_period_end, apt.last_fulfilled_appointment) as best_treatment_stop_date,

    -- Data completeness indicators
    CASE WHEN oc.obs_dose_value IS NOT NULL THEN true ELSE false END as has_structured_dose,
    CASE WHEN oc.obs_radiation_field IS NOT NULL THEN true ELSE false END as has_structured_site,
    CASE WHEN oc.obs_start_date IS NOT NULL OR src.sr_occurrence_period_start IS NOT NULL THEN true ELSE false END as has_treatment_dates,
    CASE WHEN apt.total_appointments > 0 THEN true ELSE false END as has_appointments,
    CASE WHEN cps.total_care_plans > 0 THEN true ELSE false END as has_care_plan,

    -- Data quality score (0-1)
    (
        CAST(CASE WHEN oc.obs_dose_value IS NOT NULL THEN 1 ELSE 0 END AS DOUBLE) * 0.3 +
        CAST(CASE WHEN oc.obs_radiation_field IS NOT NULL THEN 1 ELSE 0 END AS DOUBLE) * 0.3 +
        CAST(CASE WHEN oc.obs_start_date IS NOT NULL OR src.sr_occurrence_period_start IS NOT NULL THEN 1 ELSE 0 END AS DOUBLE) * 0.2 +
        CAST(CASE WHEN apt.total_appointments > 0 THEN 1 ELSE 0 END AS DOUBLE) * 0.1 +
        CAST(CASE WHEN cps.total_care_plans > 0 THEN 1 ELSE 0 END AS DOUBLE) * 0.1
    ) as data_quality_score

FROM observation_consolidated oc
FULL OUTER JOIN service_request_courses src
    ON oc.patient_fhir_id = src.patient_fhir_id
LEFT JOIN service_request_notes srn ON src.service_request_id = srn.service_request_id
LEFT JOIN service_request_reason_codes srrc ON src.service_request_id = srrc.service_request_id
LEFT JOIN service_request_body_sites srbs ON src.service_request_id = srbs.service_request_id
LEFT JOIN appointment_summary apt
    ON COALESCE(oc.patient_fhir_id, src.patient_fhir_id) = apt.patient_fhir_id
LEFT JOIN care_plan_summary cps
    ON COALESCE(oc.patient_fhir_id, src.patient_fhir_id) = cps.patient_fhir_id
WHERE COALESCE(oc.patient_fhir_id, src.patient_fhir_id, apt.patient_fhir_id, cps.patient_fhir_id) IS NOT NULL

ORDER BY patient_fhir_id, obs_course_line_number, best_treatment_start_date;