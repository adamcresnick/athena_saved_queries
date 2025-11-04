CREATE OR REPLACE VIEW fhir_prd_db.v_imaging_corticosteroid_use AS

WITH medication_timing_bounds AS (
    -- Aggregate timing bounds from dosage instruction sub-schema
    SELECT
        medication_request_id,
        MIN(dosage_instruction_timing_repeat_bounds_period_start) as earliest_bounds_start,
        MAX(dosage_instruction_timing_repeat_bounds_period_end) as latest_bounds_end
    FROM fhir_prd_db.medication_request_dosage_instruction
    WHERE dosage_instruction_timing_repeat_bounds_period_start IS NOT NULL
       OR dosage_instruction_timing_repeat_bounds_period_end IS NOT NULL
    GROUP BY medication_request_id
),

corticosteroid_medications AS (
    -- Identify all corticosteroid medications (systemic use)
    SELECT DISTINCT
        mr.id as medication_request_fhir_id,
        mr.subject_reference as patient_fhir_id,

        -- Medication identification
        COALESCE(m.code_text, mr.medication_reference_display) as medication_name,
        mcc.code_coding_code as rxnorm_cui,
        mcc.code_coding_display as rxnorm_display,

        -- Standardized generic name (maps to RxNorm ingredient level)
        CASE
            -- High priority glucocorticoids
            WHEN mcc.code_coding_code = '3264'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%dexamethasone%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%decadron%'
                THEN 'dexamethasone'
            WHEN mcc.code_coding_code = '8640'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%prednisone%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%deltasone%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%rayos%'
                THEN 'prednisone'
            WHEN mcc.code_coding_code = '8638'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%prednisolone%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%orapred%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%prelone%'
                THEN 'prednisolone'
            WHEN mcc.code_coding_code = '6902'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%methylprednisolone%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%medrol%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%solu-medrol%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%solumedrol%'
                THEN 'methylprednisolone'
            WHEN mcc.code_coding_code = '5492'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%hydrocortisone%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%cortef%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%solu-cortef%'
                THEN 'hydrocortisone'
            WHEN mcc.code_coding_code IN ('1514', '1347')  -- Both CUIs found in data
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%betamethasone%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%celestone%'
                THEN 'betamethasone'
            WHEN mcc.code_coding_code = '10759'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%triamcinolone%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%kenalog%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%aristospan%'
                THEN 'triamcinolone'

            -- Medium priority glucocorticoids
            WHEN mcc.code_coding_code = '2878'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%cortisone%'
                THEN 'cortisone'
            WHEN mcc.code_coding_code = '22396'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%deflazacort%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%emflaza%'
                THEN 'deflazacort'
            WHEN mcc.code_coding_code = '7910'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%paramethasone%'
                THEN 'paramethasone'
            WHEN mcc.code_coding_code = '29523'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%meprednisone%'
                THEN 'meprednisone'
            WHEN mcc.code_coding_code = '4463'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%fluocortolone%'
                THEN 'fluocortolone'
            WHEN mcc.code_coding_code = '55681'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%rimexolone%'
                THEN 'rimexolone'

            -- Lower priority glucocorticoids (rare)
            WHEN mcc.code_coding_code = '12473'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%prednylidene%'
                THEN 'prednylidene'
            WHEN mcc.code_coding_code = '21285'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%cloprednol%'
                THEN 'cloprednol'
            WHEN mcc.code_coding_code = '21660'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%cortivazol%'
                THEN 'cortivazol'
            WHEN mcc.code_coding_code = '2669799'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%vamorolone%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%agamree%'
                THEN 'vamorolone'

            -- Mineralocorticoids
            WHEN mcc.code_coding_code = '4452'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%fludrocortisone%'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%florinef%'
                THEN 'fludrocortisone'
            WHEN mcc.code_coding_code = '3256'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%desoxycorticosterone%'
                THEN 'desoxycorticosterone'
            WHEN mcc.code_coding_code = '1312358'
                 OR LOWER(COALESCE(m.code_text, mr.medication_reference_display)) LIKE '%aldosterone%'
                THEN 'aldosterone'

            ELSE 'other_corticosteroid'
        END as corticosteroid_generic_name,

        -- Detection method
        CASE
            WHEN mcc.code_coding_code IS NOT NULL THEN 'rxnorm_cui'
            ELSE 'text_match'
        END as detection_method,

        -- Temporal fields - hierarchical date selection
        TRY(CAST(CASE
            WHEN mtb.earliest_bounds_start IS NOT NULL THEN
                CASE
                    WHEN LENGTH(mtb.earliest_bounds_start) = 10
                    THEN mtb.earliest_bounds_start || 'T00:00:00Z'
                    ELSE mtb.earliest_bounds_start
                END
            WHEN LENGTH(mr.authored_on) = 10
                THEN mr.authored_on || 'T00:00:00Z'
            ELSE mr.authored_on
        END AS TIMESTAMP(3))) as medication_start_datetime,

        TRY(CAST(CASE
            WHEN mtb.latest_bounds_end IS NOT NULL THEN
                CASE
                    WHEN LENGTH(mtb.latest_bounds_end) = 10
                    THEN mtb.latest_bounds_end || 'T00:00:00Z'
                    ELSE mtb.latest_bounds_end
                END
            WHEN mr.dispense_request_validity_period_end IS NOT NULL THEN
                CASE
                    WHEN LENGTH(mr.dispense_request_validity_period_end) = 10
                    THEN mr.dispense_request_validity_period_end || 'T00:00:00Z'
                    ELSE mr.dispense_request_validity_period_end
                END
            ELSE NULL
        END AS TIMESTAMP(3))) as medication_stop_datetime,

        mr.status as medication_status

    FROM fhir_prd_db.medication_request mr
    LEFT JOIN medication_timing_bounds mtb ON mr.id = mtb.medication_request_id
    LEFT JOIN fhir_prd_db.medication m
        ON m.id = mr.medication_reference_reference
    LEFT JOIN fhir_prd_db.medication_code_coding mcc
        ON mcc.medication_id = m.id
        AND mcc.code_coding_system = 'http://www.nlm.nih.gov/research/umls/rxnorm'

    WHERE (
        -- ====================================================================
        -- COMPLETE RxNorm CUI List from RxClass API (ATC H02AB + H02AA)
        -- Source: https://rxnav.nlm.nih.gov/REST/rxclass/classMembers.json
        -- Query Date: 2025-10-18
        -- Total: 20 corticosteroid ingredients (TTY=IN)
        -- ====================================================================

        -- GLUCOCORTICOIDS (ATC H02AB) - 17 ingredients
        mcc.code_coding_code IN (
            -- High Priority (Common in neuro-oncology)
            '3264',    -- dexamethasone *** MOST COMMON ***
            '8640',    -- prednisone
            '8638',    -- prednisolone
            '6902',    -- methylprednisolone
            '5492',    -- hydrocortisone
            '1514',    -- betamethasone (NOTE: API shows 1514, not 1347)
            '10759',   -- triamcinolone

            -- Medium Priority (Less common but systemic)
            '2878',    -- cortisone
            '22396',   -- deflazacort
            '7910',    -- paramethasone
            '29523',   -- meprednisone
            '4463',    -- fluocortolone
            '55681',   -- rimexolone

            -- Lower Priority (Rare/specialized)
            '12473',   -- prednylidene
            '21285',   -- cloprednol
            '21660',   -- cortivazol
            '2669799'  -- vamorolone (newest - approved 2020 for Duchenne MD)
        )

        -- MINERALOCORTICOIDS (ATC H02AA) - 3 ingredients
        OR mcc.code_coding_code IN (
            '4452',    -- fludrocortisone (most common mineralocorticoid)
            '3256',    -- desoxycorticosterone
            '1312358'  -- aldosterone
        )

        -- ====================================================================
        -- TEXT MATCHING (Fallback for medications without RxNorm codes)
        -- ====================================================================

        -- Generic names (most common)
        OR LOWER(m.code_text) LIKE '%dexamethasone%'
        OR LOWER(m.code_text) LIKE '%prednisone%'
        OR LOWER(m.code_text) LIKE '%prednisolone%'
        OR LOWER(m.code_text) LIKE '%methylprednisolone%'
        OR LOWER(m.code_text) LIKE '%hydrocortisone%'
        OR LOWER(m.code_text) LIKE '%betamethasone%'
        OR LOWER(m.code_text) LIKE '%triamcinolone%'
        OR LOWER(m.code_text) LIKE '%cortisone%'
        OR LOWER(m.code_text) LIKE '%fludrocortisone%'
        OR LOWER(m.code_text) LIKE '%deflazacort%'

        -- Generic names (less common)
        OR LOWER(m.code_text) LIKE '%paramethasone%'
        OR LOWER(m.code_text) LIKE '%meprednisone%'
        OR LOWER(m.code_text) LIKE '%fluocortolone%'
        OR LOWER(m.code_text) LIKE '%rimexolone%'
        OR LOWER(m.code_text) LIKE '%prednylidene%'
        OR LOWER(m.code_text) LIKE '%cloprednol%'
        OR LOWER(m.code_text) LIKE '%cortivazol%'
        OR LOWER(m.code_text) LIKE '%vamorolone%'
        OR LOWER(m.code_text) LIKE '%desoxycorticosterone%'
        OR LOWER(m.code_text) LIKE '%aldosterone%'

        -- Brand names (high priority)
        OR LOWER(m.code_text) LIKE '%decadron%'
        OR LOWER(m.code_text) LIKE '%medrol%'
        OR LOWER(m.code_text) LIKE '%solu-medrol%'
        OR LOWER(m.code_text) LIKE '%solumedrol%'
        OR LOWER(m.code_text) LIKE '%deltasone%'
        OR LOWER(m.code_text) LIKE '%rayos%'
        OR LOWER(m.code_text) LIKE '%orapred%'
        OR LOWER(m.code_text) LIKE '%prelone%'
        OR LOWER(m.code_text) LIKE '%cortef%'
        OR LOWER(m.code_text) LIKE '%solu-cortef%'
        OR LOWER(m.code_text) LIKE '%celestone%'
        OR LOWER(m.code_text) LIKE '%kenalog%'
        OR LOWER(m.code_text) LIKE '%aristospan%'
        OR LOWER(m.code_text) LIKE '%florinef%'
        OR LOWER(m.code_text) LIKE '%emflaza%'
        OR LOWER(m.code_text) LIKE '%agamree%'  -- vamorolone brand

        -- Same patterns for medication_reference_display
        OR LOWER(mr.medication_reference_display) LIKE '%dexamethasone%'
        OR LOWER(mr.medication_reference_display) LIKE '%decadron%'
        OR LOWER(mr.medication_reference_display) LIKE '%prednisone%'
        OR LOWER(mr.medication_reference_display) LIKE '%prednisolone%'
        OR LOWER(mr.medication_reference_display) LIKE '%methylprednisolone%'
        OR LOWER(mr.medication_reference_display) LIKE '%medrol%'
        OR LOWER(mr.medication_reference_display) LIKE '%solu-medrol%'
        OR LOWER(mr.medication_reference_display) LIKE '%hydrocortisone%'
        OR LOWER(mr.medication_reference_display) LIKE '%cortef%'
        OR LOWER(mr.medication_reference_display) LIKE '%betamethasone%'
        OR LOWER(mr.medication_reference_display) LIKE '%celestone%'
        OR LOWER(mr.medication_reference_display) LIKE '%triamcinolone%'
        OR LOWER(mr.medication_reference_display) LIKE '%kenalog%'
        OR LOWER(mr.medication_reference_display) LIKE '%fludrocortisone%'
        OR LOWER(mr.medication_reference_display) LIKE '%florinef%'
        OR LOWER(mr.medication_reference_display) LIKE '%deflazacort%'
        OR LOWER(mr.medication_reference_display) LIKE '%emflaza%'
    )
    AND mr.status IN ('active', 'completed', 'stopped', 'on-hold')
),

imaging_corticosteroid_matches AS (
    -- Match imaging studies to corticosteroid medications
    SELECT
        img.patient_fhir_id,
        img.imaging_procedure_id,
        img.imaging_date,
        img.imaging_modality,
        img.imaging_procedure,

        cm.medication_request_fhir_id,
        cm.medication_name,
        cm.rxnorm_cui,
        cm.rxnorm_display,
        cm.corticosteroid_generic_name,
        cm.detection_method,
        cm.medication_start_datetime,
        cm.medication_stop_datetime,
        cm.medication_status,

        -- Calculate temporal relationship
        DATE_DIFF('day',
            DATE(CAST(cm.medication_start_datetime AS TIMESTAMP)),
            DATE(CAST(img.imaging_date AS TIMESTAMP))
        ) as days_from_med_start_to_imaging,

        CASE
            WHEN cm.medication_stop_datetime IS NOT NULL THEN
                DATE_DIFF('day',
                    DATE(CAST(img.imaging_date AS TIMESTAMP)),
                    DATE(CAST(cm.medication_stop_datetime AS TIMESTAMP))
                )
            ELSE NULL
        END as days_from_imaging_to_med_stop,

        -- Temporal relationship categories
        -- UPDATED: Only captures corticosteroid use AT imaging or within 7 days PRIOR
        -- No future use (after imaging) is captured
        CASE
            WHEN DATE(CAST(img.imaging_date AS TIMESTAMP))
                 BETWEEN DATE(CAST(cm.medication_start_datetime AS TIMESTAMP))
                     AND COALESCE(DATE(CAST(cm.medication_stop_datetime AS TIMESTAMP)),
                                  DATE(CAST(img.imaging_date AS TIMESTAMP)))
                THEN 'on_corticosteroid_at_imaging'
            WHEN DATE(CAST(img.imaging_date AS TIMESTAMP))
                 BETWEEN DATE(CAST(cm.medication_start_datetime AS TIMESTAMP)) - INTERVAL '7' DAY
                     AND DATE(CAST(cm.medication_start_datetime AS TIMESTAMP)) - INTERVAL '1' DAY
                THEN 'within_7days_prior_to_imaging'
            WHEN DATE(CAST(img.imaging_date AS TIMESTAMP))
                 > COALESCE(DATE(CAST(cm.medication_stop_datetime AS TIMESTAMP)),
                           DATE(CAST(cm.medication_start_datetime AS TIMESTAMP)))
                 AND DATE_DIFF('day',
                              COALESCE(DATE(CAST(cm.medication_stop_datetime AS TIMESTAMP)),
                                      DATE(CAST(cm.medication_start_datetime AS TIMESTAMP))),
                              DATE(CAST(img.imaging_date AS TIMESTAMP))) <= 7
                THEN 'within_7days_after_stop'
            ELSE 'outside_window'
        END as temporal_relationship

    FROM fhir_prd_db.v_imaging img
    LEFT JOIN corticosteroid_medications cm
        ON img.patient_fhir_id = cm.patient_fhir_id
        -- Apply temporal filter: medication active at imaging OR within 7 days prior
        -- Only captures PAST or CURRENT use, not FUTURE use
        AND (
            -- Case 1: Medication active at imaging date
            (DATE(CAST(img.imaging_date AS TIMESTAMP))
             BETWEEN DATE(CAST(cm.medication_start_datetime AS TIMESTAMP))
                 AND COALESCE(DATE(CAST(cm.medication_stop_datetime AS TIMESTAMP)),
                             DATE(CAST(img.imaging_date AS TIMESTAMP))))
            OR
            -- Case 2: Medication stopped within 7 days before imaging
            (cm.medication_stop_datetime IS NOT NULL
             AND DATE(CAST(img.imaging_date AS TIMESTAMP))
                 BETWEEN DATE(CAST(cm.medication_stop_datetime AS TIMESTAMP))
                     AND DATE(CAST(cm.medication_stop_datetime AS TIMESTAMP)) + INTERVAL '7' DAY)
            OR
            -- Case 3: Medication started within 7 days before imaging (but not yet at imaging)
            (DATE(CAST(img.imaging_date AS TIMESTAMP))
             BETWEEN DATE(CAST(cm.medication_start_datetime AS TIMESTAMP)) - INTERVAL '7' DAY
                 AND DATE(CAST(cm.medication_start_datetime AS TIMESTAMP)) - INTERVAL '1' DAY)
        )
),

corticosteroid_counts AS (
    -- Count concurrent corticosteroids per imaging study
    -- UPDATED: Only counts corticosteroids active at imaging or within 7 days prior
    SELECT
        imaging_procedure_id,
        COUNT(DISTINCT corticosteroid_generic_name) as total_corticosteroids_count,
        LISTAGG(DISTINCT corticosteroid_generic_name, '; ')
            WITHIN GROUP (ORDER BY corticosteroid_generic_name) as corticosteroid_list
    FROM imaging_corticosteroid_matches
    WHERE temporal_relationship IN ('on_corticosteroid_at_imaging', 'within_7days_prior_to_imaging', 'within_7days_after_stop')
    GROUP BY imaging_procedure_id
)

-- Main query
SELECT
    icm.patient_fhir_id,
    icm.imaging_procedure_id,
    icm.imaging_date,
    icm.imaging_modality,
    icm.imaging_procedure,

    -- Corticosteroid exposure flag
    -- UPDATED: TRUE only if patient was on corticosteroid at imaging or within 7 days prior
    CASE
        WHEN icm.medication_request_fhir_id IS NOT NULL
             AND icm.temporal_relationship IN ('on_corticosteroid_at_imaging', 'within_7days_prior_to_imaging', 'within_7days_after_stop')
            THEN true
        ELSE false
    END as on_corticosteroid,

    -- Corticosteroid details
    icm.medication_request_fhir_id as corticosteroid_medication_fhir_id,
    icm.medication_name as corticosteroid_name,
    icm.rxnorm_cui as corticosteroid_rxnorm_cui,
    icm.rxnorm_display as corticosteroid_rxnorm_display,
    icm.corticosteroid_generic_name,
    icm.detection_method,

    -- Temporal details
    icm.medication_start_datetime,
    icm.medication_stop_datetime,
    icm.days_from_med_start_to_imaging,
    icm.days_from_imaging_to_med_stop,
    icm.temporal_relationship,
    icm.medication_status,

    -- Aggregated counts
    COALESCE(cc.total_corticosteroids_count, 0) as total_corticosteroids_count,
    cc.corticosteroid_list

FROM imaging_corticosteroid_matches icm
LEFT JOIN corticosteroid_counts cc
    ON icm.imaging_procedure_id = cc.imaging_procedure_id

ORDER BY icm.patient_fhir_id, icm.imaging_date DESC, icm.corticosteroid_generic_name;