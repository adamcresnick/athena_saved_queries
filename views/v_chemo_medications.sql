CREATE OR REPLACE VIEW fhir_prd_db.v_chemo_medications AS
WITH
-- ================================================================================
-- Step 1: Get timing bounds from dosage instruction (provides ~89% date coverage)
-- ================================================================================
medication_timing_bounds AS (
    SELECT
        medication_request_id,
        MIN(dosage_instruction_timing_repeat_bounds_period_start) as earliest_bounds_start,
        MAX(dosage_instruction_timing_repeat_bounds_period_end) as latest_bounds_end
    FROM fhir_prd_db.medication_request_dosage_instruction
    WHERE dosage_instruction_timing_repeat_bounds_period_start IS NOT NULL
       OR dosage_instruction_timing_repeat_bounds_period_end IS NOT NULL
    GROUP BY medication_request_id
),

-- ================================================================================
-- Step 2: Get all medication RxNorm codes (both ingredient and product codes)
-- ================================================================================
medication_rxnorm_codes AS (
    SELECT
        mcc.medication_id,
        mcc.code_coding_code AS rxnorm_code,
        mcc.code_coding_display AS rxnorm_display
    FROM fhir_prd_db.medication_code_coding mcc
    WHERE mcc.code_coding_system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
        AND mcc.code_coding_code IS NOT NULL
),

-- ================================================================================
-- Step 3a: Match RxNorm codes to chemotherapy drugs (BOTH ingredient AND product)
-- ================================================================================
chemotherapy_rxnorm_matches AS (
    SELECT DISTINCT
        mrc.medication_id,
        mrc.rxnorm_code,
        mrc.rxnorm_display,
        -- Direct ingredient match
        COALESCE(cd_direct.drug_id, cd_product.drug_id) AS chemo_drug_id,
        COALESCE(cd_direct.preferred_name, cd_product.preferred_name) AS chemo_preferred_name,
        COALESCE(cd_direct.approval_status, cd_product.approval_status) AS chemo_approval_status,
        COALESCE(cd_direct.rxnorm_in, cd_product.rxnorm_in) AS chemo_rxnorm_ingredient,
        COALESCE(cd_direct.ncit_code, cd_product.ncit_code) AS chemo_ncit_code,
        COALESCE(cd_direct.sources, cd_product.sources) AS chemo_sources,
        COALESCE(cd_direct.drug_category, cd_product.drug_category) AS chemo_drug_category,
        COALESCE(cd_direct.therapeutic_normalized, cd_product.therapeutic_normalized) AS chemo_therapeutic_normalized,
        -- Match type for debugging
        CASE
            WHEN cd_direct.drug_id IS NOT NULL THEN 'rxnorm_ingredient'
            WHEN cd_product.drug_id IS NOT NULL THEN 'rxnorm_product'
            ELSE 'unknown'
        END AS match_type
    FROM medication_rxnorm_codes mrc
    -- Try direct ingredient code match first
    LEFT JOIN fhir_prd_db.v_chemotherapy_drugs cd_direct
        ON mrc.rxnorm_code = cd_direct.rxnorm_in
    -- Try product→ingredient mapping if no direct match
    LEFT JOIN fhir_prd_db.v_chemotherapy_rxnorm_codes crc
        ON mrc.rxnorm_code = crc.product_rxnorm_code
    LEFT JOIN fhir_prd_db.v_chemotherapy_drugs cd_product
        ON crc.ingredient_rxnorm_code = cd_product.rxnorm_in
    WHERE cd_direct.drug_id IS NOT NULL
       OR cd_product.drug_id IS NOT NULL
),

-- ================================================================================
-- Step 3b: Name-based matching for medications WITHOUT RxNorm matches (NEW!)
-- ================================================================================
-- First, get any RxNorm codes that exist for these medications
medications_without_chemo_match AS (
    SELECT DISTINCT m.id as medication_id
    FROM fhir_prd_db.medication m
    WHERE m.id NOT IN (SELECT medication_id FROM chemotherapy_rxnorm_matches)
),

name_matched_medications AS (
    SELECT DISTINCT
        m.id as medication_id,
        cd.drug_id,
        cd.preferred_name,
        cd.approval_status,
        cd.rxnorm_in,
        cd.ncit_code,
        cd.sources,
        cd.drug_category,
        cd.therapeutic_normalized
    FROM fhir_prd_db.medication m
    INNER JOIN medications_without_chemo_match mwcm ON m.id = mwcm.medication_id
    CROSS JOIN fhir_prd_db.v_chemotherapy_drugs cd
    WHERE
        -- Exclude short drug names (<=3 chars) to prevent spurious substring matches
        -- 2-letter codes: "AC" matching "acetaminophen", "AG" matching "magnesium"
        -- 3-letter codes: "ADE" matching "ropivacaine", "lidocaine", "adenosine"
        -- These codes will ONLY match via RxNorm (if they have RxNorm codes)
        LENGTH(cd.preferred_name) > 3
        -- Exclude placeholder/generic drug names (2025-10-26)
        -- Prevents "nonspecific investigational medication" from matching generic terms
        AND cd.preferred_name NOT LIKE 'Drugs Approved for%'
        AND cd.preferred_name NOT LIKE 'Access to Experimental%'
        AND cd.preferred_name NOT LIKE 'Drug-based intervention%'
        AND cd.preferred_name NOT LIKE '%placebo%'
        AND cd.preferred_name NOT LIKE '%Placebo%'
        AND cd.preferred_name NOT LIKE 'Experimental:%'
        AND cd.preferred_name NOT LIKE 'Investigational Chemotherapy%'
        AND cd.preferred_name NOT LIKE 'Combination drug'
        AND cd.preferred_name NOT LIKE '%drug chemotherapy group'
        AND cd.preferred_name NOT LIKE 'ntifier:%'
        AND cd.preferred_name != '***'
        -- Match on medication name (case-insensitive, partial match)
        AND (
            -- Try exact preferred name match first
            LOWER(m.code_text) LIKE '%' || LOWER(cd.preferred_name) || '%'
            -- Also try matching common variations
            OR (cd.preferred_name = 'vincristine' AND LOWER(m.code_text) LIKE '%vincrist%')
            OR (cd.preferred_name = 'carboplatin' AND LOWER(m.code_text) LIKE '%carbopl%')
            OR (cd.preferred_name = 'cisplatin' AND LOWER(m.code_text) LIKE '%cispl%')
            OR (cd.preferred_name = 'cyclophosphamide' AND LOWER(m.code_text) LIKE '%cycloph%')
            OR (cd.preferred_name = 'temozolomide' AND LOWER(m.code_text) LIKE '%temozol%')
            OR (cd.preferred_name = 'methotrexate' AND LOWER(m.code_text) LIKE '%methotr%')
            OR (cd.preferred_name = 'doxorubicin' AND LOWER(m.code_text) LIKE '%doxorub%')
            OR (cd.preferred_name = 'etoposide' AND LOWER(m.code_text) LIKE '%etopos%')
            OR (cd.preferred_name = 'ifosfamide' AND LOWER(m.code_text) LIKE '%ifosfam%')
            OR (cd.preferred_name = 'cytarabine' AND LOWER(m.code_text) LIKE '%cytarab%')
            OR (cd.preferred_name = 'lomustine' AND LOWER(m.code_text) LIKE '%lomustin%')
            OR (cd.preferred_name = 'procarbazine' AND LOWER(m.code_text) LIKE '%procarb%')
            OR (cd.preferred_name = 'thiotepa' AND LOWER(m.code_text) LIKE '%thiotepa%')
        )
),

-- ================================================================================
-- Step 3c: Extract investigational drug names from notes (NEW!)
-- ================================================================================
investigational_drug_extraction AS (
    SELECT DISTINCT
        mr.medication_reference_reference as medication_id,
        -- Extract drug name after "Name of investigational drug:"
        TRIM(
            SUBSTRING(
                SUBSTRING(mrn.note_text, POSITION('Name of investigational drug:' IN mrn.note_text) + 30),
                1,
                CASE
                    WHEN POSITION(' ' IN SUBSTRING(mrn.note_text, POSITION('Name of investigational drug:' IN mrn.note_text) + 30)) > 0
                    THEN POSITION(' ' IN SUBSTRING(mrn.note_text, POSITION('Name of investigational drug:' IN mrn.note_text) + 30)) - 1
                    ELSE 50  -- Default max length if no space found
                END
            )
        ) as extracted_drug_name
    FROM fhir_prd_db.medication_request mr
    LEFT JOIN fhir_prd_db.medication_request_note mrn
        ON mr.id = mrn.medication_request_id
    WHERE mr.medication_reference_display LIKE '%nonspecific%investigational%'
        AND mrn.note_text IS NOT NULL
        AND LOWER(mrn.note_text) LIKE '%name of investigational drug:%'
),

investigational_with_extracted_names AS (
    SELECT DISTINCT
        ide.medication_id,
        NULL as rxnorm_code,
        NULL as rxnorm_display,
        cd.drug_id AS chemo_drug_id,
        COALESCE(cd.preferred_name, ide.extracted_drug_name) AS chemo_preferred_name,
        COALESCE(cd.approval_status, 'investigational') AS chemo_approval_status,
        cd.rxnorm_in AS chemo_rxnorm_ingredient,
        cd.ncit_code AS chemo_ncit_code,
        COALESCE(cd.sources, 'CLINICAL_TRIAL') AS chemo_sources,
        COALESCE(cd.drug_category, 'investigational_therapy') AS chemo_drug_category,
        cd.therapeutic_normalized AS chemo_therapeutic_normalized,
        'investigational_extracted' AS match_type
    FROM investigational_drug_extraction ide
    INNER JOIN medications_without_chemo_match mwcm ON ide.medication_id = mwcm.medication_id
    -- Try to match extracted name to reference table (EXACT match only)
    -- FIXED 2025-10-26: Removed LIKE matching to prevent single drug names from matching combo-strings
    -- Previously: "imatinib" would match "Imatinib, Fluorouracil, Oxaliplatin..." causing false combo entries
    LEFT JOIN fhir_prd_db.v_chemotherapy_drugs cd
        ON LOWER(cd.preferred_name) = LOWER(ide.extracted_drug_name)
    -- Exclude from name_matched_medications (prevent duplicates)
    WHERE ide.medication_id NOT IN (SELECT medication_id FROM name_matched_medications)
),

-- ================================================================================
-- Step 3d: Generic investigational/oncology medication matches (fallback)
-- ================================================================================
generic_investigational_matches AS (
    SELECT DISTINCT
        m.id as medication_id,
        NULL as rxnorm_code,
        NULL as rxnorm_display,
        NULL AS chemo_drug_id,
        'Investigational Chemotherapy (unspecified)' AS chemo_preferred_name,
        'investigational' AS chemo_approval_status,
        NULL AS chemo_rxnorm_ingredient,
        NULL AS chemo_ncit_code,
        'CLINICAL_TRIAL' AS chemo_sources,
        'investigational_therapy' AS chemo_drug_category,
        'investigational chemotherapy (unspecified)' AS chemo_therapeutic_normalized,
        'investigational_generic' AS match_type
    FROM fhir_prd_db.medication m
    INNER JOIN medications_without_chemo_match mwcm ON m.id = mwcm.medication_id
    WHERE
        -- Match generic investigational/oncology patterns
        (
            -- Investigational medications
            (LOWER(m.code_text) LIKE '%nonspecific%' AND LOWER(m.code_text) LIKE '%onco%' AND LOWER(m.code_text) LIKE '%investigational%')
            OR (LOWER(m.code_text) LIKE '%nonformulary%' AND LOWER(m.code_text) LIKE '%oncology%')
            OR (LOWER(m.code_text) LIKE '%investigational%' AND LOWER(m.code_text) LIKE '%onco%')
            OR LOWER(m.code_text) LIKE '%oncology outpatient medication%'
        )
        -- Exclude from name_matched_medications and investigational_with_extracted_names
        AND m.id NOT IN (SELECT medication_id FROM name_matched_medications)
        AND m.id NOT IN (SELECT medication_id FROM investigational_with_extracted_names)
),

-- Get RxNorm codes for name-matched medications (if they exist)
name_matched_rxnorm_codes AS (
    SELECT
        nmm.medication_id,
        mcc.code_coding_code as rxnorm_code,
        mcc.code_coding_display as rxnorm_display
    FROM name_matched_medications nmm
    LEFT JOIN fhir_prd_db.medication_code_coding mcc
        ON nmm.medication_id = mcc.medication_id
        AND mcc.code_coding_system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
        AND mcc.code_coding_code IS NOT NULL
),

chemotherapy_name_matches AS (
    SELECT DISTINCT
        nmm.medication_id,
        -- Include RxNorm codes if they exist for this medication
        nmrc.rxnorm_code,
        nmrc.rxnorm_display,
        nmm.drug_id AS chemo_drug_id,
        nmm.preferred_name AS chemo_preferred_name,
        nmm.approval_status AS chemo_approval_status,
        nmm.rxnorm_in AS chemo_rxnorm_ingredient,
        nmm.ncit_code AS chemo_ncit_code,
        nmm.sources AS chemo_sources,
        nmm.drug_category AS chemo_drug_category,
        nmm.therapeutic_normalized AS chemo_therapeutic_normalized,
        'name_match' AS match_type
    FROM name_matched_medications nmm
    LEFT JOIN name_matched_rxnorm_codes nmrc
        ON nmm.medication_id = nmrc.medication_id
),

-- ================================================================================
-- Step 3e: UNION all matching strategies
-- ================================================================================
chemotherapy_medication_matches AS (
    SELECT * FROM chemotherapy_rxnorm_matches
    UNION ALL
    SELECT * FROM chemotherapy_name_matches
    UNION ALL
    SELECT * FROM investigational_with_extracted_names
    UNION ALL
    SELECT * FROM generic_investigational_matches
),

-- ================================================================================
-- Step 4: Aggregate medication details (same as v_medications)
-- ================================================================================
medication_notes AS (
    SELECT
        medication_request_id,
        LISTAGG(note_text, ' | ') WITHIN GROUP (ORDER BY note_text) as note_text_aggregated
    FROM fhir_prd_db.medication_request_note
    GROUP BY medication_request_id
),
medication_reasons AS (
    SELECT
        medication_request_id,
        LISTAGG(reason_code_text, ' | ') WITHIN GROUP (ORDER BY reason_code_text) as reason_code_text_aggregated
    FROM fhir_prd_db.medication_request_reason_code
    GROUP BY medication_request_id
),
medication_forms AS (
    SELECT
        medication_id,
        LISTAGG(DISTINCT form_coding_code, ' | ') WITHIN GROUP (ORDER BY form_coding_code) as form_coding_codes,
        LISTAGG(DISTINCT form_coding_display, ' | ') WITHIN GROUP (ORDER BY form_coding_display) as form_coding_displays
    FROM fhir_prd_db.medication_form_coding
    GROUP BY medication_id
),
medication_ingredients AS (
    SELECT
        medication_id,
        LISTAGG(DISTINCT CAST(ingredient_strength_numerator_value AS VARCHAR) || ' ' || ingredient_strength_numerator_unit, ' | ')
            WITHIN GROUP (ORDER BY CAST(ingredient_strength_numerator_value AS VARCHAR) || ' ' || ingredient_strength_numerator_unit) as ingredient_strengths
    FROM fhir_prd_db.medication_ingredient
    WHERE ingredient_strength_numerator_value IS NOT NULL
    GROUP BY medication_id
),
medication_dosage_instructions AS (
    SELECT
        medication_request_id,
        -- Route information (CRITICAL for chemotherapy analysis)
        LISTAGG(DISTINCT dosage_instruction_route_text, ' | ') WITHIN GROUP (ORDER BY dosage_instruction_route_text) as route_text_aggregated,
        -- Method (e.g., IV push, IV drip)
        LISTAGG(DISTINCT dosage_instruction_method_text, ' | ') WITHIN GROUP (ORDER BY dosage_instruction_method_text) as method_text_aggregated,
        -- Full dosage instruction text
        LISTAGG(dosage_instruction_text, ' | ') WITHIN GROUP (ORDER BY dosage_instruction_sequence) as dosage_text_aggregated,
        -- Site (e.g., port, peripheral line)
        LISTAGG(DISTINCT dosage_instruction_site_text, ' | ') WITHIN GROUP (ORDER BY dosage_instruction_site_text) as site_text_aggregated,
        -- Patient instructions
        LISTAGG(DISTINCT dosage_instruction_patient_instruction, ' | ') WITHIN GROUP (ORDER BY dosage_instruction_patient_instruction) as patient_instruction_aggregated,
        -- Timing information
        LISTAGG(DISTINCT dosage_instruction_timing_code_text, ' | ') WITHIN GROUP (ORDER BY dosage_instruction_timing_code_text) as timing_code_aggregated
    FROM fhir_prd_db.medication_request_dosage_instruction
    GROUP BY medication_request_id
),
medication_based_on AS (
    SELECT
        medication_request_id,
        LISTAGG(DISTINCT based_on_reference, ' | ') WITHIN GROUP (ORDER BY based_on_reference) AS based_on_references,
        LISTAGG(DISTINCT based_on_display, ' | ') WITHIN GROUP (ORDER BY based_on_display) AS based_on_displays,
        -- Use MAX to get CarePlan ID that matches care_plan.id (IDs starting with 'f' vs 'e')
        -- Testing showed MIN() selects IDs starting with 'e' which don't exist in care_plan table,
        -- while MAX() selects IDs starting with 'f' which DO exist (4,517 matches)
        MAX(based_on_reference) AS primary_care_plan_id
    FROM fhir_prd_db.medication_request_based_on
    GROUP BY medication_request_id
),
medication_reason_references AS (
    SELECT
        medication_request_id,
        LISTAGG(DISTINCT reason_reference_display, ' | ') WITHIN GROUP (ORDER BY reason_reference_display) AS reason_reference_displays
    FROM fhir_prd_db.medication_request_reason_reference
    GROUP BY medication_request_id
),

-- ================================================================================
-- ADDED 2025-10-28: CarePlan metadata CTEs for episode construction
-- These provide treatment protocol information for grouping medications into episodes
-- ================================================================================
care_plan_categories AS (
    SELECT
        care_plan_id,
        LISTAGG(DISTINCT category_text, ' | ') WITHIN GROUP (ORDER BY category_text) as categories_aggregated
    FROM fhir_prd_db.care_plan_category
    GROUP BY care_plan_id
),
care_plan_conditions AS (
    SELECT
        care_plan_id,
        LISTAGG(DISTINCT addresses_display, ' | ') WITHIN GROUP (ORDER BY addresses_display) as addresses_aggregated
    FROM fhir_prd_db.care_plan_addresses
    GROUP BY care_plan_id
),
care_plan_activity_agg AS (
    SELECT
        care_plan_id,
        LISTAGG(DISTINCT activity_detail_status, ' | ')
            WITHIN GROUP (ORDER BY activity_detail_status) AS activity_detail_statuses,
        LISTAGG(DISTINCT activity_detail_description, ' | ')
            WITHIN GROUP (ORDER BY activity_detail_description) AS activity_detail_descriptions
    FROM fhir_prd_db.care_plan_activity
    GROUP BY care_plan_id
)

-- ================================================================================
-- Step 5: Final SELECT - All chemotherapy medications with full details
-- ================================================================================
SELECT
    -- Patient identifiers
    mr.subject_reference as patient_fhir_id,
    mr.encounter_reference as encounter_fhir_id,

    -- ============================================================================
    -- ADDED 2025-10-28: Episode linkage fields for treatment episode construction
    -- ============================================================================
    -- Encounter reference (already available as encounter_fhir_id, adding mr_ prefix version for consistency)
    mr.encounter_reference as mr_encounter_reference,

    -- Treatment cycle grouping (1.2% coverage - useful when populated)
    mr.group_identifier_value as mr_group_identifier_value,
    mr.group_identifier_system as mr_group_identifier_system,

    -- Medication request identifiers
    mr.id as medication_request_fhir_id,
    m.id as medication_fhir_id,

    -- Chemotherapy classification (from comprehensive reference)
    cmm.chemo_drug_id,
    cmm.chemo_preferred_name,
    cmm.chemo_approval_status,
    cmm.chemo_rxnorm_ingredient,
    cmm.chemo_ncit_code,
    cmm.chemo_sources,
    cmm.chemo_drug_category,
    cmm.chemo_therapeutic_normalized,
    cmm.match_type as rxnorm_match_type,

    -- Medication details
    COALESCE(m.code_text, mr.medication_reference_display) as medication_name,
    cmm.rxnorm_code as medication_rxnorm_code,
    cmm.rxnorm_display as medication_rxnorm_display,

    -- Status and intent
    mr.status as medication_status,
    mr.intent as medication_intent,
    mr.priority as medication_priority,

    -- Dates (improved coverage using timing_bounds) - return as VARCHAR to avoid casting issues
    CASE
        WHEN mtb.earliest_bounds_start IS NOT NULL THEN mtb.earliest_bounds_start
        WHEN mr.authored_on IS NOT NULL THEN mr.authored_on
        ELSE NULL
    END as medication_start_date,

    CASE
        WHEN mtb.latest_bounds_end IS NOT NULL THEN mtb.latest_bounds_end
        WHEN mr.dispense_request_validity_period_end IS NOT NULL THEN mr.dispense_request_validity_period_end
        ELSE NULL
    END as medication_stop_date,

    mr.authored_on as medication_authored_date,

    -- Dosage and route (CRITICAL for chemotherapy)
    mdi.route_text_aggregated as medication_route,
    mdi.method_text_aggregated as medication_method,
    mdi.site_text_aggregated as medication_site,
    mdi.dosage_text_aggregated as medication_dosage_instructions,
    mdi.timing_code_aggregated as medication_timing,
    mdi.patient_instruction_aggregated as medication_patient_instructions,

    -- Form and ingredients
    mf.form_coding_codes as medication_form_codes,
    mf.form_coding_displays as medication_forms,
    mi.ingredient_strengths as medication_ingredient_strengths,

    -- Clinical context
    mrrefs.reason_reference_displays as medication_reason,
    mrr.reason_code_text_aggregated as medication_reason_codes,
    mn.note_text_aggregated as medication_notes,

    -- Care plan linkage
    mbo.based_on_references as care_plan_references,
    mbo.based_on_displays as care_plan_displays,

    -- Requester
    mr.requester_reference as requester_fhir_id,
    mr.requester_display as requester_name,

    -- Recorder and entered by
    mr.recorder_reference as recorder_fhir_id,
    mr.recorder_display as recorder_name,

    -- ============================================================================
    -- ADDED 2025-10-28: Additional MedicationRequest metadata for completeness
    -- ============================================================================
    TRY(CAST(mr.dispense_request_validity_period_start AS TIMESTAMP(3))) as mr_validity_period_start,
    mr.status_reason_text as mr_status_reason_text,
    mr.do_not_perform as mr_do_not_perform,
    mr.course_of_therapy_type_text as mr_course_of_therapy_type_text,
    mr.dispense_request_initial_fill_duration_value as mr_dispense_initial_fill_duration_value,
    mr.dispense_request_initial_fill_duration_unit as mr_dispense_initial_fill_duration_unit,
    mr.dispense_request_expected_supply_duration_value as mr_dispense_expected_supply_duration_value,
    mr.dispense_request_expected_supply_duration_unit as mr_dispense_expected_supply_duration_unit,
    mr.dispense_request_number_of_repeats_allowed as mr_dispense_number_of_repeats_allowed,
    mr.substitution_allowed_boolean as mr_substitution_allowed_boolean,
    mr.substitution_reason_text as mr_substitution_reason_text,
    mr.prior_prescription_display as mr_prior_prescription_display,

    -- ============================================================================
    -- ADDED 2025-10-28: CarePlan metadata for treatment episode construction
    -- CRITICAL: cp_period_start and cp_period_end define episode boundaries
    -- ============================================================================
    cp.id as cp_id,
    cp.title as cp_title,
    cp.status as cp_status,
    cp.intent as cp_intent,
    COALESCE(
        TRY(date_parse(cp.created, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(cp.created, '%Y-%m-%d'))
    ) as cp_created,
    COALESCE(
        TRY(date_parse(cp.period_start, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(cp.period_start, '%Y-%m-%d'))
    ) as cp_period_start,
    COALESCE(
        TRY(date_parse(cp.period_end, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(cp.period_end, '%Y-%m-%d'))
    ) as cp_period_end,
    cp.author_display as cp_author_display,

    -- Care plan categories (treatment type classification)
    cpc.categories_aggregated as cpc_categories_aggregated,

    -- Care plan conditions (what conditions the plan addresses)
    cpcon.addresses_aggregated as cpcon_addresses_aggregated,

    -- Care plan activity (treatment activities and their status)
    cpa.activity_detail_statuses as cpa_activity_detail_statuses,
    cpa.activity_detail_descriptions as cpa_activity_detail_descriptions

FROM fhir_prd_db.medication_request mr

-- Join to medication details (LEFT JOIN since some medications don't link to medication table)
LEFT JOIN fhir_prd_db.medication m
    ON m.id = mr.medication_reference_reference

-- Join to chemotherapy matches (INNER JOIN to filter to chemo only)
-- Now includes BOTH RxNorm matches AND name-based matches
INNER JOIN chemotherapy_medication_matches cmm
    ON cmm.medication_id = m.id

-- Join to timing bounds
LEFT JOIN medication_timing_bounds mtb
    ON mtb.medication_request_id = mr.id

-- Join to aggregated details
LEFT JOIN medication_notes mn
    ON mn.medication_request_id = mr.id
LEFT JOIN medication_reasons mrr
    ON mrr.medication_request_id = mr.id
LEFT JOIN medication_forms mf
    ON mf.medication_id = m.id
LEFT JOIN medication_ingredients mi
    ON mi.medication_id = m.id
LEFT JOIN medication_dosage_instructions mdi
    ON mdi.medication_request_id = mr.id
LEFT JOIN medication_based_on mbo
    ON mbo.medication_request_id = mr.id
LEFT JOIN medication_reason_references mrrefs
    ON mrrefs.medication_request_id = mr.id

-- ================================================================================
-- ADDED 2025-10-28: CarePlan JOINs for episode construction
-- ================================================================================
LEFT JOIN fhir_prd_db.care_plan cp
    ON mbo.primary_care_plan_id = cp.id
LEFT JOIN care_plan_categories cpc
    ON cp.id = cpc.care_plan_id
LEFT JOIN care_plan_conditions cpcon
    ON cp.id = cpcon.care_plan_id
LEFT JOIN care_plan_activity_agg cpa
    ON cp.id = cpa.care_plan_id

-- ================================================================================
-- FILTERS: Status + Exclude Supportive Care
-- ================================================================================
WHERE mr.status IN ('active', 'completed', 'stopped', 'on-hold')
    -- Exclude supportive care medications (analgesics, antiemetics, anesthetics, etc.)
    -- Keep all therapeutic categories: chemotherapy, targeted_therapy, immunotherapy,
    -- hormone_therapy, investigational_therapy, investigational_other
    AND (
        cmm.chemo_drug_category NOT IN ('supportive_care')
        OR cmm.chemo_drug_category IS NULL  -- Keep investigational_generic and uncategorized
    )
;
;