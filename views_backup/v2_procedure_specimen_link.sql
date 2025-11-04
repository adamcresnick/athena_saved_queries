CREATE OR REPLACE VIEW fhir_prd_db.v_procedure_specimen_link AS
SELECT DISTINCT
    p.id AS procedure_id,
    s.id AS specimen_id,
    p.encounter_reference AS encounter_id
FROM
    fhir_prd_db.procedure p
JOIN
    fhir_prd_db.service_request sr ON p.encounter_reference = sr.encounter_reference
JOIN
    fhir_prd_db.service_request_specimen srs ON sr.id = srs.service_request_id
JOIN
    fhir_prd_db.specimen s ON REPLACE(srs.specimen_reference, 'Specimen/', '') = s.id
WHERE
    ABS(DATE_DIFF('day', CAST(NULLIF(p.performed_period_start, '') AS DATE), CAST(NULLIF(s.collection_collected_date_time, '') AS DATE))) <= 1
