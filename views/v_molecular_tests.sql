CREATE OR REPLACE VIEW fhir_prd_db.v_molecular_tests AS
WITH aggregated_results AS (
    SELECT
        test_id,
        dgd_id,
        COUNT(*) as component_count,
        SUM(LENGTH(COALESCE(test_result_narrative, ''))) as total_narrative_chars,
        LISTAGG(DISTINCT test_component, '; ')
            WITHIN GROUP (ORDER BY test_component) as components_list
    FROM fhir_prd_db.molecular_test_results
    GROUP BY test_id, dgd_id
),
specimen_linkage AS (
    -- Aggregate multiple specimens per test to prevent JOIN explosion
    SELECT
        ar.test_id,
        MIN(ar.dgd_id) AS dgd_id,
        MIN(sri.service_request_id) AS service_request_id,
        MIN(sr.encounter_reference) AS encounter_reference,
        MIN(REPLACE(sr.encounter_reference, 'Encounter/', '')) AS encounter_id,
        MIN(s.id) AS specimen_id,
        LISTAGG(DISTINCT s.type_text, ' | ') WITHIN GROUP (ORDER BY s.type_text) AS specimen_types,
        LISTAGG(DISTINCT s.collection_body_site_text, ' | ') WITHIN GROUP (ORDER BY s.collection_body_site_text) AS specimen_sites,
        MIN(s.collection_collected_date_time) AS specimen_collection_date,
        MIN(s.accession_identifier_value) AS specimen_accession,
        -- Aggregate all specimen IDs (Athena doesn't support json_arrayagg)
        LISTAGG(DISTINCT s.id, ' | ') WITHIN GROUP (ORDER BY s.id) AS specimen_ids
    FROM aggregated_results ar
    LEFT JOIN fhir_prd_db.service_request_identifier sri
        ON ar.dgd_id = sri.identifier_value
    LEFT JOIN fhir_prd_db.service_request sr
        ON sri.service_request_id = sr.id
    LEFT JOIN fhir_prd_db.service_request_specimen srs
        ON sr.id = srs.service_request_id
    LEFT JOIN fhir_prd_db.specimen s
        ON REPLACE(srs.specimen_reference, 'Specimen/', '') = s.id
    GROUP BY ar.test_id
),
procedure_linkage AS (
    SELECT
        sl.test_id,
        MIN(p.id) AS procedure_id,
        MIN(p.code_text) AS procedure_name,
        MIN(p.performed_date_time) AS procedure_date,
        MIN(p.status) AS procedure_status
    FROM specimen_linkage sl
    LEFT JOIN fhir_prd_db.procedure p
        ON sl.encounter_id = REPLACE(p.encounter_reference, 'Encounter/', '')
        AND p.code_text LIKE '%SURGICAL%'
    WHERE sl.encounter_id IS NOT NULL
    GROUP BY sl.test_id
)
SELECT
    mt.patient_id as patient_fhir_id,
    mt.test_id as mt_test_id,
    CAST(FROM_ISO8601_TIMESTAMP(mt.result_datetime) AS DATE) as mt_test_date,
    TRY(DATE_DIFF('day',
        DATE(pa.birth_date),
        CAST(FROM_ISO8601_TIMESTAMP(mt.result_datetime) AS DATE))) as age_at_test_days,
    mt.lab_test_name as mt_lab_test_name,
    mt.lab_test_status as mt_test_status,
    mt.lab_test_requester as mt_test_requester,
    COALESCE(ar.component_count, 0) as mtr_component_count,
    COALESCE(ar.total_narrative_chars, 0) as mtr_total_narrative_chars,
    COALESCE(ar.components_list, 'None') as mtr_components_list,
    sl.specimen_id as mt_specimen_id,
    sl.specimen_ids as mt_specimen_ids,
    sl.specimen_types as mt_specimen_types,
    sl.specimen_sites as mt_specimen_sites,
    TRY(CAST(SUBSTR(sl.specimen_collection_date, 1, 10) AS TIMESTAMP(3))) as mt_specimen_collection_date,
    sl.specimen_accession as mt_specimen_accession,
    sl.encounter_id as mt_encounter_id,
    pl.procedure_id as mt_procedure_id,
    pl.procedure_name as mt_procedure_name,
    COALESCE(
        TRY(date_parse(SUBSTR(pl.procedure_date, 1, 10), '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(SUBSTR(pl.procedure_date, 1, 10), '%Y-%m-%d'))
    ) as mt_procedure_date,
    pl.procedure_status as mt_procedure_status
FROM fhir_prd_db.molecular_tests mt
LEFT JOIN aggregated_results ar ON mt.test_id = ar.test_id
LEFT JOIN specimen_linkage sl ON mt.test_id = sl.test_id
LEFT JOIN procedure_linkage pl ON mt.test_id = pl.test_id
LEFT JOIN fhir_prd_db.patient_access pa ON mt.patient_id = pa.id
WHERE mt.patient_id IS NOT NULL
ORDER BY mt.result_datetime, mt.test_id;