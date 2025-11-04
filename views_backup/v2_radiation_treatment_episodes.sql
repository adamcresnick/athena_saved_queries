CREATE OR REPLACE VIEW fhir_prd_db.v_radiation_treatment_episodes AS

-- ============================================================================
-- STRATEGY A: STRUCTURED COURSE ID EPISODES
-- ============================================================================
-- Source: Structured observations from ELECT intake forms
-- Coverage: 93 patients, 95 episodes
-- Reliability: HIGH - Explicit course_id grouping
-- ============================================================================

WITH strategy_a_base AS (
    SELECT DISTINCT
        patient_fhir_id,
        course_id,
        obs_start_date,
        obs_stop_date,
        obs_dose_value,
        obs_radiation_field,
        obs_radiation_site_code
    FROM fhir_prd_db.v_radiation_treatments
    WHERE course_id IS NOT NULL
),

strategy_a_episodes AS (
    SELECT
        patient_fhir_id,
        course_id as episode_id,
        'structured_course_id' as episode_detection_method,

        -- Episode temporal boundaries
        MIN(obs_start_date) as episode_start_datetime,
        MAX(obs_stop_date) as episode_end_datetime,
        TRY(CAST(MIN(obs_start_date) AS DATE)) as episode_start_date,
        TRY(CAST(MAX(obs_stop_date) AS DATE)) as episode_end_date,

        -- Date source tracking
        'observation.effective_date_time' as episode_start_date_source,
        'observation_component.stop_date' as episode_end_date_source,

        -- Dose aggregation
        SUM(obs_dose_value) as total_dose_cgy,
        ROUND(AVG(obs_dose_value), 2) as avg_dose_cgy,
        MIN(obs_dose_value) as min_dose_cgy,
        MAX(obs_dose_value) as max_dose_cgy,
        COUNT(obs_dose_value) as num_dose_records,

        -- Radiation fields and sites (using actual column names from v_radiation_treatments)
        COUNT(DISTINCT obs_radiation_field) as num_unique_fields,
        COUNT(DISTINCT obs_radiation_site_code) as num_unique_sites,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT obs_radiation_field), ', ') as radiation_fields,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT CAST(obs_radiation_site_code AS VARCHAR)), ', ') as radiation_site_codes,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT obs_radiation_field), ', ') as radiation_sites_summary,

        -- Data availability flags
        CAST(1 AS BOOLEAN) as has_structured_dose,
        CAST(1 AS BOOLEAN) as has_structured_field,
        CAST(1 AS BOOLEAN) as has_structured_site,
        CAST(1 AS BOOLEAN) as has_episode_dates,
        CAST(0 AS BOOLEAN) as has_appointments,  -- Enriched separately
        CAST(0 AS BOOLEAN) as has_documents,     -- Enriched separately

        -- Source-specific dates for transparency
        MIN(obs_start_date) as obs_start_date,
        MAX(obs_stop_date) as obs_stop_date,
        CAST(NULL AS TIMESTAMP(3)) as cp_start_date,
        CAST(NULL AS TIMESTAMP(3)) as cp_end_date,
        CAST(NULL AS TIMESTAMP(3)) as apt_first_date,
        CAST(NULL AS TIMESTAMP(3)) as apt_last_date,
        CAST(NULL AS TIMESTAMP(3)) as doc_earliest_date,
        CAST(NULL AS TIMESTAMP(3)) as doc_latest_date,

        -- Document metrics (not available for Strategy A)
        CAST(0 AS INTEGER) as num_documents,
        CAST(0 AS INTEGER) as priority_1_docs,
        CAST(0 AS INTEGER) as priority_2_docs,
        CAST(0 AS INTEGER) as priority_3_docs,
        CAST(0 AS INTEGER) as priority_4_docs,
        CAST(0 AS INTEGER) as priority_5_docs,
        CAST(NULL AS INTEGER) as highest_priority_available,
        CAST(NULL AS VARCHAR) as document_types,
        CAST(NULL AS VARCHAR) as document_categories,
        CAST(NULL AS ARRAY(VARCHAR)) as constituent_document_ids

    FROM strategy_a_base
    GROUP BY patient_fhir_id, course_id
),

-- ============================================================================
-- STRATEGY D: DOCUMENT TEMPORAL CLUSTERING
-- ============================================================================
-- Source: Document temporal clustering (30-day gap)
-- Coverage: 406 patients with radiation documents but NO structured course_id
-- Reliability: MEDIUM - Heuristic based on gap threshold
-- ============================================================================

documents_with_dates AS (
    SELECT
        patient_fhir_id,
        document_id,
        doc_date,
        doc_type_text,
        extraction_priority,
        document_category
    FROM fhir_prd_db.v_radiation_documents
    WHERE doc_date IS NOT NULL
),

-- Exclude patients already covered by Strategy A
patients_needing_document_episodes AS (
    SELECT DISTINCT patient_fhir_id
    FROM documents_with_dates
    WHERE patient_fhir_id NOT IN (
        SELECT DISTINCT patient_fhir_id
        FROM fhir_prd_db.v_radiation_treatments
        WHERE course_id IS NOT NULL
    )
),

-- Filter to only documents for patients needing episodes
filtered_documents AS (
    SELECT d.*
    FROM documents_with_dates d
    INNER JOIN patients_needing_document_episodes p
        ON d.patient_fhir_id = p.patient_fhir_id
),

-- Calculate gaps between consecutive documents per patient
document_with_prev AS (
    SELECT
        patient_fhir_id,
        document_id,
        doc_date,
        doc_type_text,
        extraction_priority,
        document_category,
        LAG(TRY(CAST(doc_date AS DATE)))
            OVER (PARTITION BY patient_fhir_id ORDER BY doc_date) as prev_doc_date
    FROM filtered_documents
),

-- Mark new episodes when gap > 30 days
document_with_gap_flag AS (
    SELECT
        patient_fhir_id,
        document_id,
        doc_date,
        doc_type_text,
        extraction_priority,
        document_category,
        prev_doc_date,
        CASE
            WHEN prev_doc_date IS NULL THEN 1
            WHEN DATE_DIFF('day', prev_doc_date, TRY(CAST(doc_date AS DATE))) > 30 THEN 1
            ELSE 0
        END as is_new_episode
    FROM document_with_prev
),

-- Assign episode numbers using cumulative sum
document_with_episode AS (
    SELECT
        patient_fhir_id,
        document_id,
        doc_date,
        doc_type_text,
        extraction_priority,
        document_category,
        SUM(is_new_episode) OVER (
            PARTITION BY patient_fhir_id
            ORDER BY doc_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as episode_number
    FROM document_with_gap_flag
),

-- Aggregate documents into episodes
strategy_d_episodes AS (
    SELECT
        patient_fhir_id,
        patient_fhir_id || '_doc_episode_' || CAST(episode_number AS VARCHAR) as episode_id,
        'document_temporal_cluster' as episode_detection_method,

        -- Episode temporal boundaries from documents
        MIN(doc_date) as episode_start_datetime,
        MAX(doc_date) as episode_end_datetime,
        TRY(CAST(MIN(doc_date) AS DATE)) as episode_start_date,
        TRY(CAST(MAX(doc_date) AS DATE)) as episode_end_date,

        -- Date source tracking
        'document_date' as episode_start_date_source,
        'document_date' as episode_end_date_source,

        -- No dose/site data for Strategy D
        CAST(NULL AS DOUBLE) as total_dose_cgy,
        CAST(NULL AS DOUBLE) as avg_dose_cgy,
        CAST(NULL AS DOUBLE) as min_dose_cgy,
        CAST(NULL AS DOUBLE) as max_dose_cgy,
        CAST(0 AS INTEGER) as num_dose_records,
        CAST(0 AS INTEGER) as num_unique_fields,
        CAST(0 AS INTEGER) as num_unique_sites,
        CAST(NULL AS VARCHAR) as radiation_fields,
        CAST(NULL AS VARCHAR) as radiation_site_codes,
        CAST(NULL AS VARCHAR) as radiation_sites_summary,

        -- Data availability flags
        CAST(0 AS BOOLEAN) as has_structured_dose,
        CAST(0 AS BOOLEAN) as has_structured_field,
        CAST(0 AS BOOLEAN) as has_structured_site,
        CAST(1 AS BOOLEAN) as has_episode_dates,
        CAST(0 AS BOOLEAN) as has_appointments,  -- Enriched separately
        CAST(1 AS BOOLEAN) as has_documents,

        -- Source-specific dates
        CAST(NULL AS TIMESTAMP(3)) as obs_start_date,
        CAST(NULL AS TIMESTAMP(3)) as obs_stop_date,
        CAST(NULL AS TIMESTAMP(3)) as cp_start_date,
        CAST(NULL AS TIMESTAMP(3)) as cp_end_date,
        CAST(NULL AS TIMESTAMP(3)) as apt_first_date,
        CAST(NULL AS TIMESTAMP(3)) as apt_last_date,
        MIN(doc_date) as doc_earliest_date,
        MAX(doc_date) as doc_latest_date,

        -- Document counts and priority distribution
        COUNT(DISTINCT document_id) as num_documents,
        COUNT(DISTINCT CASE WHEN extraction_priority = 1 THEN document_id END) as priority_1_docs,
        COUNT(DISTINCT CASE WHEN extraction_priority = 2 THEN document_id END) as priority_2_docs,
        COUNT(DISTINCT CASE WHEN extraction_priority = 3 THEN document_id END) as priority_3_docs,
        COUNT(DISTINCT CASE WHEN extraction_priority = 4 THEN document_id END) as priority_4_docs,
        COUNT(DISTINCT CASE WHEN extraction_priority = 5 THEN document_id END) as priority_5_docs,
        MIN(extraction_priority) as highest_priority_available,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT doc_type_text), ', ') as document_types,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT document_category), ', ') as document_categories,
        ARRAY_AGG(document_id ORDER BY extraction_priority, doc_date) as constituent_document_ids

    FROM document_with_episode
    GROUP BY patient_fhir_id, episode_number
),

-- ============================================================================
-- UNION ALL STRATEGIES
-- ============================================================================

unified_episodes AS (
    SELECT * FROM strategy_a_episodes
    UNION ALL
    SELECT * FROM strategy_d_episodes
)

-- ============================================================================
-- FINAL OUTPUT WITH DERIVED FIELDS
-- ============================================================================

SELECT
    patient_fhir_id,
    episode_id,
    episode_detection_method,

    -- Episode temporal boundaries
    episode_start_datetime,
    episode_end_datetime,
    episode_start_date,
    episode_end_date,
    DATE_DIFF('day', episode_start_date, episode_end_date) as episode_duration_days,

    -- Date source tracking
    episode_start_date_source,
    episode_end_date_source,

    -- Dose aggregation
    total_dose_cgy,
    avg_dose_cgy,
    min_dose_cgy,
    max_dose_cgy,
    num_dose_records,

    -- Radiation fields and sites
    num_unique_fields,
    num_unique_sites,
    radiation_fields,
    radiation_site_codes,
    radiation_sites_summary,

    -- Data availability flags
    has_structured_dose,
    has_structured_field,
    has_structured_site,
    has_episode_dates,
    has_appointments,
    has_documents,

    -- Source-specific dates for transparency
    obs_start_date,
    obs_stop_date,
    cp_start_date,
    cp_end_date,
    apt_first_date,
    apt_last_date,
    doc_earliest_date,
    doc_latest_date,

    -- Document metrics
    num_documents,
    priority_1_docs,
    priority_2_docs,
    priority_3_docs,
    priority_4_docs,
    priority_5_docs,
    highest_priority_available,
    document_types,
    document_categories,
    constituent_document_ids,

    -- NLP extraction recommendation
    CASE
        WHEN episode_detection_method = 'structured_course_id' THEN 'N/A - Structured Data Available'
        WHEN highest_priority_available = 1 THEN 'HIGH - Treatment Summary Available'
        WHEN highest_priority_available = 2 THEN 'MEDIUM - Consultation Notes Available'
        WHEN highest_priority_available = 3 THEN 'MEDIUM - Outside Summary Available'
        WHEN highest_priority_available = 4 THEN 'LOW - Progress Notes Only'
        ELSE 'VERY LOW - Other Documents Only'
    END as nlp_extraction_priority

FROM unified_episodes
ORDER BY patient_fhir_id, episode_start_datetime;