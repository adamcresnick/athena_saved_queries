CREATE OR REPLACE VIEW fhir_prd_db.v_binary_files AS
WITH document_contexts AS (
    SELECT
        document_reference_id,
        LISTAGG(DISTINCT context_encounter_reference, ' | ')
            WITHIN GROUP (ORDER BY context_encounter_reference) as encounter_references,
        LISTAGG(DISTINCT context_encounter_display, ' | ')
            WITHIN GROUP (ORDER BY context_encounter_display) as encounter_displays
    FROM fhir_prd_db.document_reference_context_encounter
    GROUP BY document_reference_id
),
document_categories AS (
    SELECT
        document_reference_id,
        LISTAGG(DISTINCT category_text, ' | ')
            WITHIN GROUP (ORDER BY category_text) as category_text
    FROM fhir_prd_db.document_reference_category
    GROUP BY document_reference_id
),
document_type_coding AS (
    SELECT
        document_reference_id,
        LISTAGG(DISTINCT type_coding_system, ' | ')
            WITHIN GROUP (ORDER BY type_coding_system) as type_coding_systems,
        LISTAGG(DISTINCT type_coding_code, ' | ')
            WITHIN GROUP (ORDER BY type_coding_code) as type_coding_codes,
        LISTAGG(DISTINCT type_coding_display, ' | ')
            WITHIN GROUP (ORDER BY type_coding_display) as type_coding_displays
    FROM fhir_prd_db.document_reference_type_coding
    GROUP BY document_reference_id
)
SELECT
    dr.id as document_reference_id,
    dr.subject_reference as patient_fhir_id,
    dr.status as dr_status,
    dr.doc_status as dr_doc_status,
    dr.type_text as dr_type_text,
    dcat.category_text as dr_category_text,
    TRY(CAST(FROM_ISO8601_TIMESTAMP(NULLIF(dr.date, '')) AS TIMESTAMP(3))) as dr_date,
    dr.description as dr_description,
    TRY(date_parse(NULLIF(dr.context_period_start, ''), '%Y-%m-%dT%H:%i:%sZ')) as dr_context_period_start,
    TRY(date_parse(NULLIF(dr.context_period_end, ''), '%Y-%m-%dT%H:%i:%sZ')) as dr_context_period_end,
    dr.context_facility_type_text as dr_facility_type,
    dr.context_practice_setting_text as dr_practice_setting,
    dr.authenticator_display as dr_authenticator,
    dr.custodian_display as dr_custodian,
    denc.encounter_references as dr_encounter_references,
    denc.encounter_displays as dr_encounter_displays,

    dtc.type_coding_systems as dr_type_coding_systems,
    dtc.type_coding_codes as dr_type_coding_codes,
    dtc.type_coding_displays as dr_type_coding_displays,

    dcont.content_attachment_url as binary_id,
    dcont.content_attachment_content_type as content_type,
    dcont.content_attachment_size as content_size_bytes,
    dcont.content_attachment_title as content_title,
    dcont.content_format_display as content_format,

    TRY(DATE_DIFF('day', DATE(pa.birth_date), DATE(FROM_ISO8601_TIMESTAMP(NULLIF(dr.date, ''))))) as age_at_document_days

FROM fhir_prd_db.document_reference dr
LEFT JOIN document_contexts denc ON dr.id = denc.document_reference_id
LEFT JOIN document_categories dcat ON dr.id = dcat.document_reference_id
LEFT JOIN document_type_coding dtc ON dr.id = dtc.document_reference_id
LEFT JOIN fhir_prd_db.document_reference_content dcont ON dr.id = dcont.document_reference_id
LEFT JOIN fhir_prd_db.patient pa ON dr.subject_reference = CONCAT('Patient/', pa.id)
WHERE dr.subject_reference IS NOT NULL
ORDER BY dr.subject_reference, dr.date DESC;