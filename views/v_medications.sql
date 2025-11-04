CREATE OR REPLACE VIEW fhir_prd_db.v_medications AS
WITH
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
        LISTAGG(DISTINCT dosage_instruction_route_text, ' | ') WITHIN GROUP (ORDER BY dosage_instruction_route_text) as route_text_aggregated,
        LISTAGG(DISTINCT dosage_instruction_method_text, ' | ') WITHIN GROUP (ORDER BY dosage_instruction_method_text) as method_text_aggregated,
        LISTAGG(dosage_instruction_text, ' | ') WITHIN GROUP (ORDER BY dosage_instruction_sequence) as dosage_text_aggregated,
        LISTAGG(DISTINCT dosage_instruction_site_text, ' | ') WITHIN GROUP (ORDER BY dosage_instruction_site_text) as site_text_aggregated,
        LISTAGG(DISTINCT dosage_instruction_patient_instruction, ' | ') WITHIN GROUP (ORDER BY dosage_instruction_patient_instruction) as patient_instruction_aggregated,
        LISTAGG(DISTINCT dosage_instruction_timing_code_text, ' | ') WITHIN GROUP (ORDER BY dosage_instruction_timing_code_text) as timing_code_aggregated
    FROM fhir_prd_db.medication_request_dosage_instruction
    GROUP BY medication_request_id
),
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
medication_based_on AS (
    SELECT
        medication_request_id,
        LISTAGG(DISTINCT based_on_reference, ' | ') WITHIN GROUP (ORDER BY based_on_reference) AS based_on_references,
        LISTAGG(DISTINCT based_on_display, ' | ') WITHIN GROUP (ORDER BY based_on_display) AS based_on_displays,
        MAX(based_on_reference) AS primary_care_plan_id
    FROM fhir_prd_db.medication_request_based_on
    GROUP BY medication_request_id
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
SELECT
    pm.patient_id as patient_fhir_id,
    pm.medication_request_id,
    pm.medication_id,
    pm.medication_name,
    pm.form_text as medication_form,
    pm.rx_norm_codes,
    TRY(date_parse(COALESCE(mtb.earliest_bounds_start, pm.authored_on), '%Y-%m-%dT%H:%i:%sZ')) as medication_start_date,
    TRY(date_parse(COALESCE(mtb.latest_bounds_end, mr.dispense_request_validity_period_end), '%Y-%m-%dT%H:%i:%sZ')) as medication_stop_date,
    pm.requester_name,
    pm.status as medication_status,
    pm.encounter_display,
    mr.encounter_reference as mr_encounter_reference,
    mr.group_identifier_value as mr_group_identifier_value,
    mr.group_identifier_system as mr_group_identifier_system,
    TRY(date_parse(mr.dispense_request_validity_period_start, '%Y-%m-%dT%H:%i:%sZ')) as mr_validity_period_start,
    TRY(date_parse(mr.dispense_request_validity_period_end, '%Y-%m-%dT%H:%i:%sZ')) as mr_validity_period_end,
    TRY(date_parse(mr.authored_on, '%Y-%m-%dT%H:%i:%sZ')) as mr_authored_on,
    mr.status as mr_status,
    mr.status_reason_text as mr_status_reason_text,
    mr.priority as mr_priority,
    mr.intent as mr_intent,
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
    mrn.note_text_aggregated as mrn_note_text_aggregated,
    mrr.reason_code_text_aggregated as mrr_reason_code_text_aggregated,
    mrb.based_on_references as mrb_care_plan_references,
    mrb.based_on_displays as mrb_care_plan_displays,
    mrb.primary_care_plan_id as mrb_primary_care_plan_id,
    mrdi.route_text_aggregated as mrdi_route_text,
    mrdi.method_text_aggregated as mrdi_method_text,
    mrdi.dosage_text_aggregated as mrdi_dosage_text,
    mrdi.site_text_aggregated as mrdi_site_text,
    mrdi.patient_instruction_aggregated as mrdi_patient_instruction,
    mrdi.timing_code_aggregated as mrdi_timing_code,
    mf.form_coding_codes as mf_form_coding_codes,
    mf.form_coding_displays as mf_form_coding_displays,
    mi.ingredient_strengths as mi_ingredient_strengths,
    cp.id as cp_id,
    cp.title as cp_title,
    cp.status as cp_status,
    cp.intent as cp_intent,
    TRY(date_parse(cp.created, '%Y-%m-%dT%H:%i:%sZ')) as cp_created,
    TRY(date_parse(cp.period_start, '%Y-%m-%dT%H:%i:%sZ')) as cp_period_start,
    TRY(date_parse(cp.period_end, '%Y-%m-%dT%H:%i:%sZ')) as cp_period_end,
    cp.author_display as cp_author_display,
    cpc.categories_aggregated as cpc_categories_aggregated,
    cpcon.addresses_aggregated as cpcon_addresses_aggregated,
    cpa.activity_detail_statuses as cpa_activity_detail_statuses,
    cpa.activity_detail_descriptions as cpa_activity_detail_descriptions
FROM fhir_prd_db.patient_medications pm
LEFT JOIN fhir_prd_db.medication_request mr ON pm.medication_request_id = mr.id
LEFT JOIN medication_timing_bounds mtb ON mr.id = mtb.medication_request_id
LEFT JOIN medication_notes mrn ON mr.id = mrn.medication_request_id
LEFT JOIN medication_reasons mrr ON mr.id = mrr.medication_request_id
LEFT JOIN medication_based_on mrb ON mr.id = mrb.medication_request_id
LEFT JOIN medication_dosage_instructions mrdi ON mr.id = mrdi.medication_request_id
LEFT JOIN medication_forms mf ON pm.medication_id = mf.medication_id
LEFT JOIN medication_ingredients mi ON pm.medication_id = mi.medication_id
LEFT JOIN fhir_prd_db.care_plan cp ON mrb.primary_care_plan_id = cp.id
LEFT JOIN care_plan_categories cpc ON cp.id = cpc.care_plan_id
LEFT JOIN care_plan_conditions cpcon ON cp.id = cpcon.care_plan_id
LEFT JOIN care_plan_activity_agg cpa ON cp.id = cpa.care_plan_id
WHERE pm.patient_id IS NOT NULL
ORDER BY pm.patient_id, pm.authored_on;