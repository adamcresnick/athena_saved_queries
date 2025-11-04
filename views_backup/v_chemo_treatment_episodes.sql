CREATE OR REPLACE VIEW fhir_prd_db.v_chemo_treatment_episodes AS

WITH

-- ============================================================================
-- Step 1: Standardize dates and add episode grouping keys
-- ============================================================================
medications_with_episode_dates AS (
    SELECT
        -- Episode grouping keys (PRIMARY: encounter-based)
        patient_fhir_id,
        mr_encounter_reference,  -- 99.9% coverage - PRIMARY grouping
        encounter_fhir_id,       -- Additional encounter field

        -- Episode dates (standardized to TIMESTAMP)
        COALESCE(
            TRY(CAST(medication_start_date AS TIMESTAMP(3))),      -- 99.3% coverage
            TRY(CAST(medication_authored_date AS TIMESTAMP(3)))     -- 100% fallback
        ) AS episode_start_datetime,

        -- Stop date LEFT AS NULL if not available (for uniform querying)
        TRY(CAST(medication_stop_date AS TIMESTAMP(3))) AS episode_stop_datetime,

        -- Individual medication identifiers
        medication_request_fhir_id,
        medication_fhir_id,

        -- Chemotherapy drug details
        chemo_drug_id,
        chemo_preferred_name,
        chemo_approval_status,
        chemo_rxnorm_ingredient,
        chemo_ncit_code,
        chemo_sources,
        chemo_drug_category,
        chemo_therapeutic_normalized,
        rxnorm_match_type,
        medication_name,
        medication_rxnorm_code,
        medication_rxnorm_display,

        -- Medication administration details (CRITICAL for note abstraction)
        medication_status,
        medication_intent,
        medication_priority,
        medication_route,
        medication_method,
        medication_site,
        medication_dosage_instructions,
        medication_timing,
        medication_patient_instructions,
        medication_form_codes,
        medication_forms,
        medication_ingredient_strengths,

        -- Clinical context (for note prioritization)
        medication_reason,
        medication_reason_codes,
        medication_notes,

        -- CarePlan linkage (32.7% coverage - supplemental grouping)
        cp_id,
        cp_title,
        cp_status,
        cp_intent,
        cp_period_start,
        cp_period_end,
        care_plan_references,
        care_plan_displays,
        cpc_categories_aggregated,
        cpcon_addresses_aggregated,

        -- Provider information
        requester_fhir_id,
        requester_name,
        recorder_fhir_id,
        recorder_name,

        -- Additional MedicationRequest metadata
        mr_validity_period_start,
        mr_status_reason_text,
        mr_do_not_perform,
        mr_course_of_therapy_type_text,
        mr_dispense_initial_fill_duration_value,
        mr_dispense_initial_fill_duration_unit,
        mr_dispense_expected_supply_duration_value,
        mr_dispense_expected_supply_duration_unit,
        mr_dispense_number_of_repeats_allowed,
        mr_substitution_allowed_boolean,
        mr_substitution_reason_text,
        mr_prior_prescription_display,

        -- Episode linkage fields (for future group identifier support)
        mr_group_identifier_value,
        mr_group_identifier_system,

        -- Raw date fields (for validation/debugging)
        medication_start_date AS raw_medication_start_date,
        medication_stop_date AS raw_medication_stop_date,
        medication_authored_date AS raw_medication_authored_date

    FROM fhir_prd_db.v_chemo_medications
),

-- ============================================================================
-- Step 2: Create episode-level aggregations
-- ============================================================================
episode_summary AS (
    SELECT
        -- Episode grouping keys
        patient_fhir_id,
        mr_encounter_reference AS episode_encounter_reference,

        -- Episode boundaries
        MIN(episode_start_datetime) AS episode_start_datetime,
        MAX(COALESCE(episode_stop_datetime, episode_start_datetime)) AS episode_end_datetime_preliminary,

        -- Episode metadata
        COUNT(DISTINCT medication_request_fhir_id) AS medication_count,
        COUNT(DISTINCT chemo_drug_id) AS unique_drug_count,
        COUNT(DISTINCT CASE WHEN episode_stop_datetime IS NOT NULL THEN medication_request_fhir_id END) AS medications_with_stop_date,

        -- Drug categories in episode
        LISTAGG(DISTINCT chemo_drug_category, ' | ') WITHIN GROUP (ORDER BY chemo_drug_category) AS episode_drug_categories,
        LISTAGG(DISTINCT chemo_preferred_name, ' | ') WITHIN GROUP (ORDER BY chemo_preferred_name) AS episode_drug_names,

        -- Episode clinical context (for note prioritization)
        LISTAGG(DISTINCT medication_route, ' | ') WITHIN GROUP (ORDER BY medication_route) AS episode_routes,
        LISTAGG(DISTINCT medication_status, ' | ') WITHIN GROUP (ORDER BY medication_status) AS episode_medication_statuses,

        -- CarePlan linkage (if available)
        MAX(cp_id) AS episode_care_plan_id,
        MAX(cp_title) AS episode_care_plan_title,
        MAX(cp_status) AS episode_care_plan_status,

        -- Data quality flags (useful for note retrieval prioritization - see comments above)
        CASE WHEN COUNT(CASE WHEN episode_stop_datetime IS NULL THEN 1 END) > 0 THEN true ELSE false END AS has_medications_without_stop_date,
        CASE WHEN COUNT(CASE WHEN medication_notes IS NOT NULL THEN 1 END) > 0 THEN true ELSE false END AS has_medication_notes

        -- NOTE: note_retrieval_priority and note_retrieval_rationale fields REMOVED
        -- See comments at top of view for prioritization logic to implement when needed

    FROM medications_with_episode_dates
    WHERE mr_encounter_reference IS NOT NULL  -- Only include medications with encounter reference
    GROUP BY patient_fhir_id, mr_encounter_reference
),

-- ============================================================================
-- Step 3: Finalize episode end dates (NULL if truly open-ended)
-- ============================================================================
episode_with_final_dates AS (
    SELECT
        *,
        -- Leave as NULL if no medications have stop dates (ongoing/open-ended episodes)
        CASE
            WHEN has_medications_without_stop_date = true
                AND medications_with_stop_date = 0
            THEN NULL
            ELSE episode_end_datetime_preliminary
        END AS episode_end_datetime
    FROM episode_summary
)

-- ============================================================================
-- Step 4: Final SELECT - Episode-level view with medication details
-- ============================================================================
SELECT
    -- Episode identifiers
    CAST(ROW_NUMBER() OVER (ORDER BY es.patient_fhir_id, es.episode_start_datetime) AS VARCHAR) AS episode_id,
    es.patient_fhir_id,
    es.episode_encounter_reference,

    -- Episode temporal boundaries
    es.episode_start_datetime,
    es.episode_end_datetime,  -- NULL if open-ended/ongoing
    DATE_DIFF('day', es.episode_start_datetime, COALESCE(es.episode_end_datetime, CURRENT_TIMESTAMP)) AS episode_duration_days,

    -- Episode composition
    es.medication_count,
    es.unique_drug_count,
    es.medications_with_stop_date,
    es.episode_drug_categories,
    es.episode_drug_names,
    es.episode_routes,
    es.episode_medication_statuses,

    -- CarePlan linkage
    es.episode_care_plan_id,
    es.episode_care_plan_title,
    es.episode_care_plan_status,

    -- Data quality indicators (useful for note retrieval prioritization - see comments above)
    es.has_medications_without_stop_date,
    es.has_medication_notes,

    -- NOTE: note_retrieval_priority and note_retrieval_rationale fields REMOVED
    -- See comments at top of view for prioritization logic to implement when needed
    -- You can calculate priority in your queries using the data quality indicators above

    -- Medication details (denormalized for easy access)
    mwed.medication_request_fhir_id,
    mwed.medication_fhir_id,
    mwed.episode_start_datetime AS medication_start_datetime,
    mwed.episode_stop_datetime AS medication_stop_datetime,
    mwed.chemo_drug_id,
    mwed.chemo_preferred_name,
    mwed.chemo_drug_category,
    mwed.medication_name,
    mwed.medication_route,
    mwed.medication_status,
    mwed.medication_dosage_instructions,
    mwed.medication_notes,
    mwed.medication_reason,

    -- Encounter linkage (CRITICAL for note retrieval)
    mwed.encounter_fhir_id,
    mwed.mr_encounter_reference AS medication_encounter_reference,

    -- Raw dates for debugging
    mwed.raw_medication_start_date,
    mwed.raw_medication_stop_date,
    mwed.raw_medication_authored_date

FROM episode_with_final_dates es
INNER JOIN medications_with_episode_dates mwed
    ON es.patient_fhir_id = mwed.patient_fhir_id
    AND es.episode_encounter_reference = mwed.mr_encounter_reference
;