CREATE OR REPLACE VIEW fhir_prd_db.v_measurements AS
WITH observations AS (
    SELECT
        subject_reference as patient_fhir_id,
        id as obs_observation_id,
        code_text as obs_measurement_type,
        value_quantity_value as obs_measurement_value,
        value_quantity_unit as obs_measurement_unit,
        TRY(CAST(effective_date_time AS TIMESTAMP(3))) as obs_measurement_date,
        TRY(CAST(issued AS TIMESTAMP(3))) as obs_issued,
        status as obs_status,
        encounter_reference as obs_encounter_reference,
        'observation' as source_table,
        CAST(NULL AS VARCHAR) as lt_test_id,
        CAST(NULL AS VARCHAR) as lt_measurement_type,
        TRY(CAST(CAST(NULL AS VARCHAR) AS TIMESTAMP(3))) as lt_measurement_date,
        CAST(NULL AS VARCHAR) as lt_status,
        CAST(NULL AS VARCHAR) as lt_result_diagnostic_report_id,
        CAST(NULL AS VARCHAR) as lt_lab_test_requester,
        CAST(NULL AS VARCHAR) as ltr_test_component,
        CAST(NULL AS VARCHAR) as ltr_value_string,
        CAST(NULL AS VARCHAR) as ltr_measurement_value,
        CAST(NULL AS VARCHAR) as ltr_measurement_unit,
        CAST(NULL AS VARCHAR) as ltr_value_codeable_concept_text,
        CAST(NULL AS VARCHAR) as ltr_value_range_low_value,
        CAST(NULL AS VARCHAR) as ltr_value_range_low_unit,
        CAST(NULL AS VARCHAR) as ltr_value_range_high_value,
        CAST(NULL AS VARCHAR) as ltr_value_range_high_unit,
        CAST(NULL AS VARCHAR) as ltr_value_boolean,
        CAST(NULL AS VARCHAR) as ltr_value_integer,
        CAST(NULL AS VARCHAR) as ltr_components_json,
        CAST(NULL AS VARCHAR) as ltr_components_list,
        CAST(NULL AS VARCHAR) as ltr_components_with_values
    FROM fhir_prd_db.observation
    WHERE subject_reference IS NOT NULL
),
lab_tests_aggregated AS (
    -- Aggregate lab test components to prevent JOIN explosion (one row per test)
    SELECT
        lt.patient_id,
        lt.test_id,
        lt.lab_test_name,
        lt.result_datetime,
        lt.lab_test_status,
        lt.result_diagnostic_report_id,
        lt.lab_test_requester,
        -- Aggregate components using LISTAGG (Athena doesn't support json_arrayagg)
        -- Cannot use DISTINCT with complex ORDER BY, so remove DISTINCT
        LISTAGG(ltr.test_component, ' | ')
            WITHIN GROUP (ORDER BY ltr.test_component) AS components_list,
        LISTAGG(
            CONCAT(ltr.test_component, ': ', COALESCE(CAST(ltr.value_quantity_value AS VARCHAR), ltr.value_string), ' ', COALESCE(ltr.value_quantity_unit, '')),
            ' | '
        ) WITHIN GROUP (ORDER BY ltr.test_component) AS components_with_values
    FROM fhir_prd_db.lab_tests lt
    LEFT JOIN fhir_prd_db.lab_test_results ltr
           ON lt.test_id = ltr.test_id
    WHERE lt.patient_id IS NOT NULL
    GROUP BY lt.patient_id, lt.test_id, lt.lab_test_name, lt.result_datetime,
             lt.lab_test_status, lt.result_diagnostic_report_id, lt.lab_test_requester
),
lab_tests_with_results AS (
    SELECT
        lta.patient_id as patient_fhir_id,
        CAST(NULL AS VARCHAR) as obs_observation_id,
        CAST(NULL AS VARCHAR) as obs_measurement_type,
        CAST(NULL AS VARCHAR) as obs_measurement_value,
        CAST(NULL AS VARCHAR) as obs_measurement_unit,
        TRY(CAST(CAST(NULL AS VARCHAR) AS TIMESTAMP(3))) as obs_measurement_date,
        TRY(CAST(CAST(NULL AS VARCHAR) AS TIMESTAMP(3))) as obs_issued,
        CAST(NULL AS VARCHAR) as obs_status,
        CAST(NULL AS VARCHAR) as obs_encounter_reference,
        'lab_tests' as source_table,
        lta.test_id as lt_test_id,
        lta.lab_test_name as lt_measurement_type,
        CAST(FROM_ISO8601_TIMESTAMP(lta.result_datetime) AS TIMESTAMP(3)) as lt_measurement_date,
        lta.lab_test_status as lt_status,
        lta.result_diagnostic_report_id as lt_result_diagnostic_report_id,
        lta.lab_test_requester as lt_lab_test_requester,
        lta.components_list as ltr_components_list,
        lta.components_with_values as ltr_components_with_values,
        -- Legacy single-value fields set to NULL (components are aggregated now)
        CAST(NULL AS VARCHAR) as ltr_test_component,
        CAST(NULL AS VARCHAR) as ltr_components_json,
        CAST(NULL AS VARCHAR) as ltr_value_string,
        CAST(NULL AS VARCHAR) as ltr_measurement_value,
        CAST(NULL AS VARCHAR) as ltr_measurement_unit,
        CAST(NULL AS VARCHAR) as ltr_value_codeable_concept_text,
        CAST(NULL AS VARCHAR) as ltr_value_range_low_value,
        CAST(NULL AS VARCHAR) as ltr_value_range_low_unit,
        CAST(NULL AS VARCHAR) as ltr_value_range_high_value,
        CAST(NULL AS VARCHAR) as ltr_value_range_high_unit,
        CAST(NULL AS VARCHAR) as ltr_value_boolean,
        CAST(NULL AS VARCHAR) as ltr_value_integer
    FROM lab_tests_aggregated lta
)
SELECT
    combined.*,
    pa.birth_date,
    TRY(DATE_DIFF('day',
        DATE(pa.birth_date),
        DATE(COALESCE(obs_measurement_date, lt_measurement_date)))) as age_at_measurement_days,
    TRY(DATE_DIFF('day',
        DATE(pa.birth_date),
        DATE(COALESCE(obs_measurement_date, lt_measurement_date))) / 365.25) as age_at_measurement_years
FROM (
    SELECT * FROM observations
    UNION ALL
    SELECT * FROM lab_tests_with_results
) combined
LEFT JOIN fhir_prd_db.patient_access pa ON combined.patient_fhir_id = pa.id
ORDER BY combined.patient_fhir_id, COALESCE(obs_measurement_date, lt_measurement_date);