CREATE OR REPLACE VIEW fhir_prd_db.v_document_reference_enriched AS
SELECT
    dr.id AS document_id,
    dr.subject_reference AS patient_fhir_id,
    dce.context_encounter_reference AS encounter_id,
    dr.date AS doc_date,
    dr.type_text AS doc_type_text,
    drc.content_attachment_url AS binary_id,
    drc.content_attachment_content_type AS content_type,
    dr.custodian_reference AS custodian_org_id,
    org.name AS custodian_org_name,
    CASE
        WHEN lower(dr.type_text) LIKE '%operative%' THEN 'operative_note'
        WHEN lower(dr.type_text) LIKE '%pathology%' THEN 'pathology_report'
        WHEN lower(dr.type_text) LIKE '%rad%' OR lower(dr.type_text) LIKE '%imag%' THEN 'imaging_report'
        WHEN lower(dr.type_text) LIKE '%discharge%' THEN 'discharge_summary'
        ELSE 'other'
    END AS document_category
FROM
    fhir_prd_db.document_reference dr
JOIN
    fhir_prd_db.document_reference_context_encounter dce ON dr.id = dce.document_reference_id
JOIN
    fhir_prd_db.document_reference_content drc ON dr.id = drc.document_reference_id
LEFT JOIN
    fhir_prd_db.organization org ON dr.custodian_reference = org.id
