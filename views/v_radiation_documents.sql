CREATE OR REPLACE VIEW fhir_prd_db.v_radiation_documents AS
WITH
-- Aggregate document categories
document_categories AS (
    SELECT
        document_reference_id,
        LISTAGG(category_text, ' | ') WITHIN GROUP (ORDER BY category_text) as category_text_aggregated,
        LISTAGG(category_coding, ' | ') WITHIN GROUP (ORDER BY category_coding) as category_coding_aggregated
    FROM (
        SELECT DISTINCT document_reference_id, category_text, category_coding
        FROM fhir_prd_db.document_reference_category
    )
    GROUP BY document_reference_id
),

-- Aggregate document authors
document_authors AS (
    SELECT
        document_reference_id,
        LISTAGG(author_reference, ' | ') WITHIN GROUP (ORDER BY author_reference) as author_references_aggregated,
        LISTAGG(author_display, ' | ') WITHIN GROUP (ORDER BY author_display) as author_displays_aggregated
    FROM (
        SELECT DISTINCT document_reference_id, author_reference, author_display
        FROM fhir_prd_db.document_reference_author
    )
    GROUP BY document_reference_id
),

-- Get document content (take most recent if multiple)
document_content AS (
    SELECT
        document_reference_id,
        content_type,
        attachment_url,
        attachment_title,
        attachment_creation,
        attachment_size
    FROM (
        SELECT
            document_reference_id,
            content_attachment_content_type as content_type,
            content_attachment_url as attachment_url,
            content_attachment_title as attachment_title,
            content_attachment_creation as attachment_creation,
            content_attachment_size as attachment_size,
            ROW_NUMBER() OVER (
                PARTITION BY document_reference_id
                ORDER BY content_attachment_creation DESC NULLS LAST,
                         content_attachment_url
            ) as rn
        FROM fhir_prd_db.document_reference_content
    )
    WHERE rn = 1
)

SELECT
    dr.subject_reference as patient_fhir_id,
    dr.id as document_id,

    -- Document metadata (doc_ prefix)
    dr.type_text as doc_type_text,
    dr.description as doc_description,
    -- FIX 2025-10-29: Use FROM_ISO8601_TIMESTAMP and cast to TIMESTAMP(3) for ISO8601 date strings
    -- Root cause: document_reference.date stores dates as '2021-11-05T20:14:59Z' (ISO8601)
    -- TRY(CAST()) returns NULL for ISO8601 strings, FROM_ISO8601_TIMESTAMP() works correctly
    -- Must wrap in CAST to TIMESTAMP(3) because FROM_ISO8601_TIMESTAMP returns 'timestamp with time zone' which Athena views don't support
    CAST(TRY(FROM_ISO8601_TIMESTAMP(dr.date)) AS TIMESTAMP(3)) as doc_date,
    dr.status as doc_status,
    dr.doc_status as doc_doc_status,
    COALESCE(
        TRY(date_parse(dr.context_period_start, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(dr.context_period_start, '%Y-%m-%d'))
    ) as doc_context_period_start,
    COALESCE(
        TRY(date_parse(dr.context_period_end, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(dr.context_period_end, '%Y-%m-%d'))
    ) as doc_context_period_end,
    dr.context_facility_type_text as doc_facility_type,
    dr.context_practice_setting_text as doc_practice_setting,

    -- Document content (docc_ prefix)
    drc.content_type as docc_content_type,
    drc.attachment_url as docc_attachment_url,
    drc.attachment_title as docc_attachment_title,
    drc.attachment_creation as docc_attachment_creation,
    drc.attachment_size as docc_attachment_size,

    -- Document category (doct_ prefix)
    drcat.category_text_aggregated as doct_category_text,
    drcat.category_coding_aggregated as doct_category_coding,

    -- Document authors (doca_ prefix)
    dra.author_references_aggregated as doca_author_references,
    dra.author_displays_aggregated as doca_author_displays,

    -- Extraction priority classification
    CASE
        WHEN dr.type_text = 'Rad Onc Treatment Report' THEN 1
        WHEN dr.type_text = 'ONC RadOnc End of Treatment' THEN 1
        WHEN LOWER(dr.description) LIKE '%end of treatment%summary%' THEN 1
        WHEN LOWER(dr.description) LIKE '%treatment summary%report%' THEN 1

        WHEN dr.type_text = 'ONC RadOnc Consult' THEN 2
        WHEN LOWER(dr.description) LIKE '%consult%' AND LOWER(dr.description) LIKE '%rad%onc%' THEN 2
        WHEN LOWER(dr.description) LIKE '%initial%consultation%' THEN 2

        WHEN dr.type_text = 'ONC Outside Summaries' AND LOWER(dr.description) LIKE '%radiation%' THEN 3
        WHEN dr.type_text = 'Clinical Report-Consult' AND LOWER(dr.description) LIKE '%radiation%' THEN 3
        WHEN dr.type_text = 'External Misc Clinical' AND LOWER(dr.description) LIKE '%radiation%' THEN 3

        WHEN LOWER(dr.description) LIKE '%progress%note%' THEN 4
        WHEN LOWER(dr.description) LIKE '%social work%' THEN 4

        ELSE 5
    END as extraction_priority,

    -- Document category for extraction type
    CASE
        WHEN dr.type_text IN ('Rad Onc Treatment Report', 'ONC RadOnc End of Treatment')
             OR LOWER(dr.description) LIKE '%end of treatment%' THEN 'Treatment Summary'
        WHEN dr.type_text = 'ONC RadOnc Consult'
             OR LOWER(dr.description) LIKE '%consult%' THEN 'Consultation'
        WHEN LOWER(dr.description) LIKE '%progress%note%' THEN 'Progress Note'
        WHEN LOWER(dr.description) LIKE '%social work%' THEN 'Social Work Note'
        WHEN dr.type_text = 'ONC Outside Summaries' THEN 'Outside Summary'
        ELSE 'Other'
    END as document_category

FROM fhir_prd_db.document_reference dr
LEFT JOIN document_content drc ON dr.id = drc.document_reference_id
LEFT JOIN document_categories drcat ON dr.id = drcat.document_reference_id
LEFT JOIN document_authors dra ON dr.id = dra.document_reference_id

WHERE dr.subject_reference IS NOT NULL
  AND (
    -- Specific radiation oncology terms (high confidence)
    LOWER(dr.type_text) LIKE '%rad%onc%'
    OR LOWER(dr.type_text) LIKE '%radonc%'
    OR LOWER(dr.type_text) LIKE '%radiation%therapy%'
    OR LOWER(dr.type_text) LIKE '%radiation%treatment%'
    OR LOWER(dr.type_text) LIKE '%radiotherapy%'
    -- Specific modalities
    OR LOWER(dr.type_text) LIKE '%imrt%'
    OR LOWER(dr.type_text) LIKE '%proton%'
    OR LOWER(dr.type_text) LIKE '%cyberknife%'
    OR LOWER(dr.type_text) LIKE '%cyber knife%'
    OR LOWER(dr.type_text) LIKE '%gamma knife%'
    OR LOWER(dr.type_text) LIKE '%gammaknife%'
    OR LOWER(dr.type_text) LIKE '%xrt%'
    OR LOWER(dr.type_text) LIKE '%x-rt%'
    -- Generic radiation (but exclude radiology)
    OR (LOWER(dr.type_text) LIKE '%radiation%'
        AND LOWER(dr.type_text) NOT LIKE '%radiology%'
        AND LOWER(dr.type_text) NOT LIKE '%diagnostic%')
    -- Same patterns for description field
    OR LOWER(dr.description) LIKE '%rad%onc%'
    OR LOWER(dr.description) LIKE '%radonc%'
    OR LOWER(dr.description) LIKE '%radiation%therapy%'
    OR LOWER(dr.description) LIKE '%radiation%treatment%'
    OR LOWER(dr.description) LIKE '%radiotherapy%'
    OR LOWER(dr.description) LIKE '%imrt%'
    OR LOWER(dr.description) LIKE '%proton%'
    OR LOWER(dr.description) LIKE '%cyberknife%'
    OR LOWER(dr.description) LIKE '%cyber knife%'
    OR LOWER(dr.description) LIKE '%gamma knife%'
    OR LOWER(dr.description) LIKE '%gammaknife%'
    OR LOWER(dr.description) LIKE '%xrt%'
    OR LOWER(dr.description) LIKE '%x-rt%'
    OR (LOWER(dr.description) LIKE '%radiation%'
        AND LOWER(dr.description) NOT LIKE '%radiology%'
        AND LOWER(dr.description) NOT LIKE '%diagnostic%')
  )

ORDER BY dr.subject_reference, extraction_priority, dr.date DESC;