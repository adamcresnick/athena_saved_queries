CREATE OR REPLACE VIEW fhir_prd_db.v_radiation_episode_enrichment AS

WITH episode_base AS (
    -- Get all episodes from unified view
    SELECT
        patient_fhir_id,
        episode_id,
        episode_detection_method,
        episode_start_date,
        episode_end_date,
        episode_duration_days,

        -- Structured data from Strategy A
        total_dose_cgy,
        radiation_fields,
        radiation_site_codes,
        num_dose_records,

        -- Document data from Strategy D
        num_documents,
        highest_priority_available,
        nlp_extraction_priority,

        -- Data availability flags
        has_structured_dose,
        has_documents

    FROM fhir_prd_db.v_radiation_treatment_episodes
),

-- ============================================================================
-- APPOINTMENT ENRICHMENT (Strategy C Revised)
-- ============================================================================
-- Purpose: Link appointments to episodes based on temporal proximity
-- Classification: Pre-treatment, during treatment, post-treatment, follow-up
-- ============================================================================

appointments_with_dates AS (
    -- Get radiation appointments with full dates from v_appointments
    SELECT
        rta.patient_fhir_id,
        va.appointment_fhir_id as appointment_id,
        va.appointment_date as appointment_date,
        TRY(CAST(NULLIF(va.appt_start, '') AS TIMESTAMP(3))) as appointment_start,
        TRY(CAST(NULLIF(va.appt_end, '') AS TIMESTAMP(3))) as appointment_end,
        va.appt_status as appointment_status,
        va.appt_appointment_type_text as appointment_type,
        va.appt_description as description,
        va.appt_comment as comment,
        va.appt_cancelation_reason_text as cancelation_reason
    FROM fhir_prd_db.v_radiation_treatment_appointments rta
    INNER JOIN fhir_prd_db.v_appointments va
        ON rta.appointment_id = va.appointment_fhir_id
    WHERE va.appointment_date IS NOT NULL
),

appointment_episode_linkage AS (
    -- Link appointments to episodes based on temporal proximity
    -- Appointment is linked if within ±1 year of episode
    SELECT
        eb.patient_fhir_id,
        eb.episode_id,

        awd.appointment_id,
        awd.appointment_date,
        awd.appointment_status,
        awd.appointment_type,

        -- Classify appointment timing relative to episode
        CASE
            WHEN awd.appointment_date < eb.episode_start_date
                AND DATE_DIFF('day', awd.appointment_date, eb.episode_start_date) <= 30
                THEN 'pre_treatment'
            WHEN awd.appointment_date BETWEEN eb.episode_start_date AND eb.episode_end_date
                THEN 'during_treatment'
            WHEN awd.appointment_date > eb.episode_end_date
                AND DATE_DIFF('day', eb.episode_end_date, awd.appointment_date) <= 30
                THEN 'post_treatment'
            WHEN awd.appointment_date > eb.episode_end_date
                AND DATE_DIFF('day', eb.episode_end_date, awd.appointment_date) <= 90
                THEN 'early_followup'
            WHEN awd.appointment_date > eb.episode_end_date
                AND DATE_DIFF('day', eb.episode_end_date, awd.appointment_date) <= 365
                THEN 'late_followup'
            ELSE 'unrelated'
        END as appointment_phase

    FROM episode_base eb
    INNER JOIN appointments_with_dates awd
        ON eb.patient_fhir_id = awd.patient_fhir_id
    WHERE
        -- Only link appointments within ±1 year of episode
        awd.appointment_date BETWEEN DATE_ADD('day', -365, eb.episode_start_date)
                                 AND DATE_ADD('day', 365, eb.episode_end_date)
        AND awd.appointment_status IN ('booked', 'fulfilled', 'arrived', 'checked-in')
),

appointment_aggregations AS (
    -- Aggregate appointment metrics by episode
    SELECT
        patient_fhir_id,
        episode_id,

        -- Overall appointment counts
        COUNT(DISTINCT appointment_id) as total_appointments,
        COUNT(DISTINCT CASE WHEN appointment_status = 'fulfilled' THEN appointment_id END) as fulfilled_appointments,
        COUNT(DISTINCT CASE WHEN appointment_status = 'booked' THEN appointment_id END) as booked_appointments,

        -- Appointment counts by phase
        COUNT(DISTINCT CASE WHEN appointment_phase = 'pre_treatment' THEN appointment_id END) as pre_treatment_appointments,
        COUNT(DISTINCT CASE WHEN appointment_phase = 'during_treatment' THEN appointment_id END) as during_treatment_appointments,
        COUNT(DISTINCT CASE WHEN appointment_phase = 'post_treatment' THEN appointment_id END) as post_treatment_appointments,
        COUNT(DISTINCT CASE WHEN appointment_phase = 'early_followup' THEN appointment_id END) as early_followup_appointments,
        COUNT(DISTINCT CASE WHEN appointment_phase = 'late_followup' THEN appointment_id END) as late_followup_appointments,

        -- Appointment types (aggregated)
        ARRAY_JOIN(ARRAY_AGG(DISTINCT appointment_type), ', ') as appointment_types,

        -- Temporal metrics
        MIN(appointment_date) as first_appointment_date,
        MAX(appointment_date) as last_appointment_date,
        MIN(CASE WHEN appointment_phase = 'pre_treatment' THEN appointment_date END) as first_pre_treatment_appointment,
        MIN(CASE WHEN appointment_phase = 'during_treatment' THEN appointment_date END) as first_during_treatment_appointment,
        MIN(CASE WHEN appointment_phase = 'post_treatment' THEN appointment_date END) as first_post_treatment_appointment,
        MIN(CASE WHEN appointment_phase = 'early_followup' THEN appointment_date END) as first_followup_appointment,

        -- Fulfillment rate
        ROUND(
            CAST(COUNT(DISTINCT CASE WHEN appointment_status = 'fulfilled' THEN appointment_id END) AS DOUBLE) /
            NULLIF(COUNT(DISTINCT appointment_id), 0) * 100,
            1
        ) as appointment_fulfillment_rate_pct

    FROM appointment_episode_linkage
    WHERE appointment_phase != 'unrelated'  -- Exclude temporally distant appointments
    GROUP BY patient_fhir_id, episode_id
),

-- ============================================================================
-- CARE PLAN ENRICHMENT (Strategy B)
-- ============================================================================
-- Purpose: Link care plans to episodes for protocol/intent metadata
-- Note: Care plan dates have low coverage (13 plans with dates)
-- Use: Protocol tracking, intent classification
-- ============================================================================

care_plan_linkage AS (
    SELECT
        eb.patient_fhir_id,
        eb.episode_id,

        cp.care_plan_id,
        cp.cp_status,
        cp.cp_intent,
        cp.cp_title,
        cp.cppo_part_of_reference,

        -- Temporal data (sparse - only 13 plans have dates)
        cp.cp_period_start,
        cp.cp_period_end

    FROM episode_base eb
    INNER JOIN fhir_prd_db.v_radiation_care_plan_hierarchy cp
        ON eb.patient_fhir_id = cp.patient_fhir_id
    WHERE
        -- Link care plans if:
        -- 1) They have dates that overlap with episode (preferred)
        -- 2) OR they exist for the patient and have radiation-related content (fallback)
        (
            -- Option 1: Temporal overlap (only works for 13 care plans with dates)
            (cp.cp_period_start IS NOT NULL
             AND cp.cp_period_end IS NOT NULL
             AND cp.cp_period_start <= eb.episode_end_date
             AND cp.cp_period_end >= eb.episode_start_date)
            OR
            -- Option 2: Fallback - link all care plans for patient if no dates
            (cp.cp_period_start IS NULL OR cp.cp_period_end IS NULL)
        )
),

care_plan_aggregations AS (
    -- Aggregate care plan metadata by episode
    SELECT
        patient_fhir_id,
        episode_id,

        -- Care plan counts
        COUNT(DISTINCT care_plan_id) as total_care_plans,
        COUNT(DISTINCT CASE WHEN cp_period_start IS NOT NULL THEN care_plan_id END) as care_plans_with_dates,

        -- Intent classification
        COUNT(DISTINCT CASE WHEN cp_intent = 'plan' THEN care_plan_id END) as care_plans_intent_plan,
        COUNT(DISTINCT CASE WHEN cp_intent = 'proposal' THEN care_plan_id END) as care_plans_intent_proposal,
        COUNT(DISTINCT CASE WHEN cp_intent = 'order' THEN care_plan_id END) as care_plans_intent_order,

        -- Status classification
        COUNT(DISTINCT CASE WHEN cp_status = 'active' THEN care_plan_id END) as care_plans_active,
        COUNT(DISTINCT CASE WHEN cp_status = 'completed' THEN care_plan_id END) as care_plans_completed,
        COUNT(DISTINCT CASE WHEN cp_status = 'draft' THEN care_plan_id END) as care_plans_draft,

        -- Protocols and descriptions (aggregated)
        ARRAY_JOIN(ARRAY_AGG(DISTINCT cp_title), ' | ') as care_plan_titles,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT cppo_part_of_reference), ', ') as care_plan_parent_references,

        -- Hierarchical structure
        COUNT(DISTINCT cppo_part_of_reference) as has_parent_care_plans,

        -- Date ranges (for care plans with dates)
        MIN(cp_period_start) as earliest_care_plan_start,
        MAX(cp_period_end) as latest_care_plan_end

    FROM care_plan_linkage
    GROUP BY patient_fhir_id, episode_id
),

-- ============================================================================
-- COMBINED ENRICHMENT
-- ============================================================================

enriched_episodes AS (
    SELECT
        eb.*,

        -- Appointment enrichment
        COALESCE(aa.total_appointments, 0) as total_appointments,
        COALESCE(aa.fulfilled_appointments, 0) as fulfilled_appointments,
        COALESCE(aa.booked_appointments, 0) as booked_appointments,
        COALESCE(aa.pre_treatment_appointments, 0) as pre_treatment_appointments,
        COALESCE(aa.during_treatment_appointments, 0) as during_treatment_appointments,
        COALESCE(aa.post_treatment_appointments, 0) as post_treatment_appointments,
        COALESCE(aa.early_followup_appointments, 0) as early_followup_appointments,
        COALESCE(aa.late_followup_appointments, 0) as late_followup_appointments,
        aa.appointment_types,
        aa.first_appointment_date,
        aa.last_appointment_date,
        aa.first_pre_treatment_appointment,
        aa.first_during_treatment_appointment,
        aa.first_post_treatment_appointment,
        aa.first_followup_appointment,
        aa.appointment_fulfillment_rate_pct,

        -- Care plan enrichment
        COALESCE(cpa.total_care_plans, 0) as total_care_plans,
        COALESCE(cpa.care_plans_with_dates, 0) as care_plans_with_dates,
        COALESCE(cpa.care_plans_intent_plan, 0) as care_plans_intent_plan,
        COALESCE(cpa.care_plans_intent_proposal, 0) as care_plans_intent_proposal,
        COALESCE(cpa.care_plans_intent_order, 0) as care_plans_intent_order,
        COALESCE(cpa.care_plans_active, 0) as care_plans_active,
        COALESCE(cpa.care_plans_completed, 0) as care_plans_completed,
        COALESCE(cpa.care_plans_draft, 0) as care_plans_draft,
        cpa.care_plan_titles,
        cpa.care_plan_parent_references,
        COALESCE(cpa.has_parent_care_plans, 0) as has_parent_care_plans,
        cpa.earliest_care_plan_start,
        cpa.latest_care_plan_end,

        -- Enrichment flags
        CAST(CASE WHEN aa.total_appointments > 0 THEN 1 ELSE 0 END AS BOOLEAN) as has_appointment_enrichment,
        CAST(CASE WHEN cpa.total_care_plans > 0 THEN 1 ELSE 0 END AS BOOLEAN) as has_care_plan_enrichment

    FROM episode_base eb
    LEFT JOIN appointment_aggregations aa
        ON eb.patient_fhir_id = aa.patient_fhir_id
        AND eb.episode_id = aa.episode_id
    LEFT JOIN care_plan_aggregations cpa
        ON eb.patient_fhir_id = cpa.patient_fhir_id
        AND eb.episode_id = cpa.episode_id
)

-- ============================================================================
-- FINAL OUTPUT WITH DERIVED METRICS
-- ============================================================================

SELECT
    *,

    -- Comprehensive enrichment score (0-100)
    CAST(
        (
            -- Base episode data (40 points)
            CASE WHEN has_structured_dose THEN 20 ELSE 0 END +
            CASE WHEN has_documents THEN 20 ELSE 0 END +

            -- Appointment enrichment (30 points)
            CASE WHEN total_appointments > 0 THEN 10 ELSE 0 END +
            CASE WHEN during_treatment_appointments > 0 THEN 10 ELSE 0 END +
            CASE WHEN appointment_fulfillment_rate_pct >= 80 THEN 10 ELSE 0 END +

            -- Care plan enrichment (30 points)
            CASE WHEN total_care_plans > 0 THEN 10 ELSE 0 END +
            CASE WHEN care_plans_with_dates > 0 THEN 10 ELSE 0 END +
            CASE WHEN care_plans_active > 0 THEN 10 ELSE 0 END
        ) AS INTEGER
    ) as enrichment_score,

    -- Data completeness category
    CASE
        WHEN has_structured_dose AND has_appointment_enrichment AND has_care_plan_enrichment
            THEN 'COMPLETE'
        WHEN has_structured_dose OR (has_appointment_enrichment AND has_care_plan_enrichment)
            THEN 'GOOD'
        WHEN has_appointment_enrichment OR has_care_plan_enrichment OR has_documents
            THEN 'PARTIAL'
        ELSE 'MINIMAL'
    END as data_completeness_tier,

    -- Treatment phase coverage (based on appointments)
    CASE
        WHEN pre_treatment_appointments > 0 AND during_treatment_appointments > 0 AND post_treatment_appointments > 0
            THEN 'FULL_CONTINUUM'
        WHEN during_treatment_appointments > 0 AND (pre_treatment_appointments > 0 OR post_treatment_appointments > 0)
            THEN 'TREATMENT_PLUS_ONE'
        WHEN during_treatment_appointments > 0
            THEN 'TREATMENT_ONLY'
        WHEN pre_treatment_appointments > 0 OR post_treatment_appointments > 0
            THEN 'CONSULTATION_ONLY'
        ELSE 'NO_APPOINTMENTS'
    END as treatment_phase_coverage

FROM enriched_episodes
ORDER BY patient_fhir_id, episode_start_date;