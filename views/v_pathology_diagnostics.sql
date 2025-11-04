CREATE OR REPLACE VIEW fhir_prd_db.v_pathology_diagnostics AS

WITH oid_reference AS (
    SELECT * FROM fhir_prd_db.v_oid_reference
),

-- ============================================================================
-- CTE 1: Tumor Surgeries (anchor point from validated v_procedures_tumor)
-- ============================================================================
tumor_surgeries AS (
    SELECT
        patient_fhir_id,
        procedure_fhir_id,
        proc_performed_date_time as surgery_datetime,
        CAST(proc_performed_date_time AS DATE) as surgery_date,
        proc_code_text as surgery_name,
        surgery_type,
        pbs_body_site_text as surgical_site,
        cpt_code as surgery_cpt,
        proc_encounter_reference as surgery_encounter
    FROM fhir_prd_db.v_procedures_tumor
    WHERE is_tumor_surgery = TRUE
      AND is_likely_performed = TRUE
),

-- ============================================================================
-- CTE 2: Surgical Specimens (linked to tumor surgeries by patient + date)
-- ============================================================================
surgical_specimens AS (
    SELECT DISTINCT
        ts.patient_fhir_id,
        ts.procedure_fhir_id,
        ts.surgery_datetime,
        ts.surgery_date,
        ts.surgery_encounter,
        s.id as specimen_id,
        s.type_text as specimen_type,
        s.collection_body_site_text as specimen_site,
        CAST(TRY(from_iso8601_timestamp(s.collection_collected_date_time)) AS TIMESTAMP(3)) as collection_datetime,
        TRY(CAST(CAST(s.collection_collected_date_time AS VARCHAR) AS DATE)) as collection_date
    FROM tumor_surgeries ts
    INNER JOIN fhir_prd_db.specimen s
        ON ts.patient_fhir_id = REPLACE(s.subject_reference, 'Patient/', '')
        AND ABS(DATE_DIFF('day',
            ts.surgery_date,
            TRY(CAST(CAST(s.collection_collected_date_time AS VARCHAR) AS DATE))
        )) <= 7  -- Within 7 days of surgery
    WHERE s.id IS NOT NULL
),

-- ============================================================================
-- CTE 3: Surgical Pathology Observations (linked via specimen_reference)
-- ============================================================================
-- NOTE: This CTE captures genomics/molecular data from observations table.
--       The separate v_molecular_tests view provides an alternate curated view
--       with aggregated components from the molecular_tests materialized table.
-- ============================================================================
surgical_pathology_observations AS (
    SELECT
        REPLACE(o.subject_reference, 'Patient/', '') as patient_fhir_id,
        'surgical_pathology_observation' as diagnostic_source,
        o.id as source_id,
        CAST(TRY(from_iso8601_timestamp(o.effective_date_time)) AS TIMESTAMP(3)) as diagnostic_datetime,
        TRY(CAST(CAST(o.effective_date_time AS VARCHAR) AS DATE)) as diagnostic_date,

        -- Observation identification
        o.code_text as diagnostic_name,
        occ.code_coding_code as code,
        oid_obs.masterfile_code as coding_system_code,
        oid_obs.description as coding_system_name,

        -- Component (use code_coding_display for detailed component name)
        COALESCE(occ.code_coding_display, o.code_text) as component_name,

        -- Result value (value_string contains free text pathology results!)
        o.value_string as result_value,

        -- Test metadata
        'Surgical Pathology Lab' as test_lab,

        -- Specimen information (from observation.specimen_display and linked specimen resource)
        COALESCE(s.type_text, o.specimen_display) as specimen_types,
        s.collection_body_site_text as specimen_sites,
        CAST(TRY(from_iso8601_timestamp(s.collection_collected_date_time)) AS TIMESTAMP(3)) as specimen_collection_datetime,

        -- Procedure linkage (link via specimen → service_request → encounter → procedure)
        p.id as linked_procedure_id,
        p.code_text as linked_procedure_name,
        CAST(TRY(from_iso8601_timestamp(p.performed_date_time)) AS TIMESTAMP(3)) as linked_procedure_datetime,

        -- Encounter linkage
        REPLACE(o.encounter_reference, 'Encounter/', '') as encounter_id,

        -- Generic categorization (LOINC/resource-based, no hardcoded features)
        CASE
            -- Use LOINC codes for categorization
            WHEN occ.code_coding_code IN ('24419-4') THEN 'Gross_Observation'
            WHEN occ.code_coding_code IN ('34574-4') THEN 'Final_Diagnosis'
            WHEN occ.code_coding_code IN ('11526-1') THEN 'Pathology_Study'

            -- Use observation code_text patterns for categorization
            WHEN LOWER(o.code_text) LIKE '%gross%' THEN 'Gross_Observation'
            WHEN LOWER(o.code_text) LIKE '%final%diagnosis%' THEN 'Final_Diagnosis'
            WHEN LOWER(o.code_text) LIKE '%genomics%interpretation%' THEN 'Genomics_Interpretation'
            WHEN LOWER(o.code_text) LIKE '%genomics%method%' THEN 'Genomics_Method'

            -- Use observation category if available
            WHEN oc.category_text = 'Laboratory' THEN 'Laboratory_Observation'
            WHEN oc.category_text = 'Imaging' THEN 'Imaging_Observation'

            -- Generic fallback
            ELSE 'Clinical_Observation'
        END as diagnostic_category,

        -- Metadata
        o.status as test_status,
        NULL as test_orderer,
        CAST(NULL AS BIGINT) as component_count,

        -- NLP prioritization (observations are structured data, not documents - no prioritization)
        CAST(NULL AS INTEGER) as extraction_priority,
        CAST(NULL AS VARCHAR) as document_category,
        CAST(NULL AS BIGINT) as days_from_surgery

    FROM fhir_prd_db.observation o

    -- Join to specimen resource via specimen_reference
    LEFT JOIN fhir_prd_db.specimen s
        ON REPLACE(o.specimen_reference, 'Specimen/', '') = s.id

    -- Link specimen → service_request → procedure (to get surgery context)
    LEFT JOIN fhir_prd_db.service_request_specimen srs
        ON s.id = REPLACE(srs.specimen_reference, 'Specimen/', '')
    LEFT JOIN fhir_prd_db.service_request sr
        ON srs.service_request_id = sr.id
    LEFT JOIN fhir_prd_db.procedure p
        ON REPLACE(sr.encounter_reference, 'Encounter/', '') = REPLACE(p.encounter_reference, 'Encounter/', '')
        AND p.code_text LIKE '%SURGICAL%'

    -- Join to observation metadata tables
    LEFT JOIN fhir_prd_db.observation_code_coding occ
        ON o.id = occ.observation_id
    LEFT JOIN fhir_prd_db.observation_category oc
        ON o.id = oc.observation_id
    LEFT JOIN oid_reference oid_obs
        ON occ.code_coding_system = oid_obs.oid_uri

    -- FILTER to confirmed tumor surgery patients only
    INNER JOIN tumor_surgeries ts
        ON REPLACE(o.subject_reference, 'Patient/', '') = ts.patient_fhir_id

    WHERE o.subject_reference IS NOT NULL
      AND o.id IS NOT NULL
      AND o.specimen_reference IS NOT NULL  -- Require specimen linkage
      AND o.value_string IS NOT NULL  -- Require free text results
      -- Filter to pathology-related observations
      AND (
          LOWER(o.code_text) LIKE '%pathology%'
          OR LOWER(o.code_text) LIKE '%surgical%consult%'
          OR LOWER(o.code_text) LIKE '%genomics%'
          OR LOWER(o.code_text) LIKE '%autopsy%'
          OR LOWER(o.code_text) LIKE '%neuropathology%'
          OR LOWER(o.code_text) LIKE '%brain%gross%'
          OR LOWER(o.code_text) LIKE '%brain%final%'
          OR LOWER(occ.code_coding_display) LIKE '%pathology%'
          OR occ.code_coding_code IN ('24419-4', '34574-4', '11526-1')  -- LOINC pathology codes
      )
),

-- ============================================================================
-- CTE 5: Surgical Pathology Reports (from surgical encounters)
-- ============================================================================
surgical_pathology_narratives AS (
    SELECT
        ts.patient_fhir_id,
        'surgical_pathology_report' as diagnostic_source,
        dr.id as source_id,
        CAST(TRY(from_iso8601_timestamp(dr.effective_date_time)) AS TIMESTAMP(3)) as diagnostic_datetime,
        TRY(CAST(CAST(dr.effective_date_time AS VARCHAR) AS DATE)) as diagnostic_date,

        -- Report identification
        dr.code_text as diagnostic_name,
        drcc.code_coding_code as code,
        NULL as coding_system_code,
        NULL as coding_system_name,

        -- Component
        dr.code_text as component_name,

        -- Narrative content (preserved as-is for downstream analysis)
        -- Include BOTH conclusion AND URL to full report for NLP workflows
        CASE
            WHEN dr.conclusion IS NOT NULL AND drpf.presented_form_url IS NOT NULL
                THEN dr.conclusion || ' | URL: ' || drpf.presented_form_url
            WHEN dr.conclusion IS NOT NULL
                THEN dr.conclusion
            WHEN drpf.presented_form_url IS NOT NULL
                THEN 'Full report at: ' || drpf.presented_form_url
            ELSE NULL
        END as result_value,

        -- Test metadata
        'Pathology Report' as test_lab,

        -- Specimen information (from surgical_specimens if linked)
        ss.specimen_type as specimen_types,
        ss.specimen_site as specimen_sites,
        ss.collection_datetime as specimen_collection_datetime,

        -- Procedure linkage (from tumor_surgeries)
        ts.procedure_fhir_id as linked_procedure_id,
        ts.surgery_name as linked_procedure_name,
        ts.surgery_datetime as linked_procedure_datetime,

        -- Encounter linkage
        REPLACE(dr.encounter_reference, 'Encounter/', '') as encounter_id,

        -- Generic categorization (resource type-based, no hardcoded features)
        CASE
            WHEN drcc.code_coding_code IN ('24419-4', '34574-4', '11526-1') THEN 'Pathology_Report'
            WHEN LOWER(dr.code_text) LIKE '%pathology%' THEN 'Pathology_Report'
            WHEN LOWER(dr.code_text) LIKE '%surgical%' THEN 'Surgical_Report'
            ELSE 'Diagnostic_Report'
        END as diagnostic_category,

        -- Metadata
        dr.status as test_status,
        NULL as test_orderer,
        CAST(NULL AS BIGINT) as component_count,

        -- ====================================================================
        -- NLP PRIORITIZATION FRAMEWORK (for document processing workflows)
        -- ====================================================================

        -- Extraction priority (1=highest, 5=lowest) - content and temporal based
        CASE
            -- Priority 1: Final surgical pathology reports (definitive diagnosis)
            WHEN LOWER(dr.code_text) LIKE '%surgical%pathology%final%' THEN 1
            WHEN LOWER(dr.code_text) LIKE '%pathology%final%diagnosis%' THEN 1
            WHEN LOWER(dr.code_text) LIKE '%final%pathology%report%' THEN 1
            WHEN drcc.code_coding_code = '34574-4' THEN 1  -- LOINC: Pathology report final diagnosis

            -- Priority 2: Surgical pathology reports (gross observations, preliminary)
            WHEN LOWER(dr.code_text) LIKE '%surgical%pathology%' THEN 2
            WHEN LOWER(dr.code_text) LIKE '%pathology%gross%' THEN 2
            WHEN drcc.code_coding_code = '24419-4' THEN 2  -- LOINC: Surgical pathology gross
            WHEN drcc.code_coding_code = '11526-1' THEN 2  -- LOINC: Pathology study

            -- Priority 3: Biopsy and specimen reports
            WHEN LOWER(dr.code_text) LIKE '%biopsy%' THEN 3
            WHEN LOWER(dr.code_text) LIKE '%specimen%' THEN 3

            -- Priority 4: Consultation notes
            WHEN LOWER(dr.code_text) LIKE '%pathology%consult%' THEN 4
            WHEN LOWER(dr.code_text) LIKE '%consult%pathology%' THEN 4

            -- Priority 5: Other diagnostic reports
            ELSE 5
        END as extraction_priority,

        -- Document category for NLP extraction type
        CASE
            WHEN LOWER(dr.code_text) LIKE '%final%' AND LOWER(dr.code_text) LIKE '%pathology%'
                THEN 'Final Pathology Report'
            WHEN LOWER(dr.code_text) LIKE '%surgical%pathology%'
                THEN 'Surgical Pathology Report'
            WHEN LOWER(dr.code_text) LIKE '%gross%'
                THEN 'Gross Pathology Observation'
            WHEN LOWER(dr.code_text) LIKE '%biopsy%'
                THEN 'Biopsy Report'
            WHEN LOWER(dr.code_text) LIKE '%consult%'
                THEN 'Pathology Consultation'
            WHEN LOWER(dr.code_text) LIKE '%specimen%'
                THEN 'Specimen Report'
            ELSE 'Other Pathology Report'
        END as document_category,

        -- Temporal relevance to surgery (days from surgery)
        ABS(DATE_DIFF('day', ts.surgery_date, TRY(CAST(CAST(dr.effective_date_time AS VARCHAR) AS DATE)))) as days_from_surgery

    FROM tumor_surgeries ts
    INNER JOIN fhir_prd_db.diagnostic_report dr
        ON REPLACE(dr.encounter_reference, 'Encounter/', '') = ts.surgery_encounter
        -- ENCOUNTER-ONLY linkage (no patient-level fallback due to performance)
        -- Captures reports explicitly linked to surgical encounters

    -- Join for code information
    LEFT JOIN fhir_prd_db.diagnostic_report_code_coding drcc
        ON dr.id = drcc.diagnostic_report_id

    -- Join for presented_form URL (for NLP workflows on binary reports)
    LEFT JOIN fhir_prd_db.diagnostic_report_presented_form drpf
        ON dr.id = drpf.diagnostic_report_id

    -- LEFT JOIN to surgical_specimens (if available for specimen details)
    LEFT JOIN surgical_specimens ss
        ON ts.patient_fhir_id = ss.patient_fhir_id
        AND ABS(DATE_DIFF('day',
            TRY(CAST(CAST(dr.effective_date_time AS VARCHAR) AS DATE)),
            ss.surgery_date
        )) <= 7

    WHERE dr.subject_reference IS NOT NULL
      AND (dr.conclusion IS NOT NULL OR drpf.presented_form_url IS NOT NULL)
      -- Filter to pathology-related reports
      AND (
          LOWER(dr.code_text) LIKE '%pathology%'
          OR LOWER(dr.code_text) LIKE '%surgical%'
          OR LOWER(dr.code_text) LIKE '%biopsy%'
          OR LOWER(dr.code_text) LIKE '%specimen%'
          OR drcc.code_coding_code IN ('24419-4', '34574-4', '11526-1')  -- LOINC pathology codes
      )
      -- Exclude reports already captured in molecular_tests
      AND NOT EXISTS (
          SELECT 1 FROM fhir_prd_db.molecular_tests mt
          WHERE mt.result_diagnostic_report_id = dr.id
      )
),

-- ============================================================================
-- CTE 6: Pathology Document References (for NLP workflows, including send-outs)
-- ============================================================================
-- NOTE: Captures ALL document_reference resources linked to surgical encounters
--       for NLP processing. Includes send-out pathology analyses, external reports, etc.
-- ============================================================================
pathology_document_references AS (
    SELECT
        ts.patient_fhir_id,
        'pathology_document' as diagnostic_source,
        dref.id as source_id,
        CAST(TRY(from_iso8601_timestamp(dref.date)) AS TIMESTAMP(3)) as diagnostic_datetime,
        TRY(CAST(CAST(dref.date AS VARCHAR) AS DATE)) as diagnostic_date,

        -- Document identification
        dref.description as diagnostic_name,
        NULL as code,
        NULL as coding_system_code,
        NULL as coding_system_name,

        -- Component
        dref.type_text as component_name,

        -- Document URL for NLP workflows (CRITICAL for send-out analyses)
        CASE
            WHEN drefc.content_attachment_url IS NOT NULL
                THEN 'Document: ' || drefc.content_attachment_url ||
                     ' | Type: ' || COALESCE(drefc.content_attachment_content_type, 'unknown') ||
                     ' | Title: ' || COALESCE(drefc.content_attachment_title, dref.description)
            ELSE dref.description
        END as result_value,

        -- Test metadata
        'Document Repository' as test_lab,

        -- Specimen information (try to link via surgical_specimens if available)
        ss.specimen_type as specimen_types,
        ss.specimen_site as specimen_sites,
        ss.collection_datetime as specimen_collection_datetime,

        -- Procedure linkage (from tumor_surgeries)
        ts.procedure_fhir_id as linked_procedure_id,
        ts.surgery_name as linked_procedure_name,
        ts.surgery_datetime as linked_procedure_datetime,

        -- Encounter linkage (via document_reference_context_encounter)
        drce.context_encounter_reference as encounter_id,

        -- Generic categorization
        CASE
            WHEN drefc.content_attachment_content_type LIKE '%pdf%' THEN 'PDF_Document'
            WHEN drefc.content_attachment_content_type LIKE '%image%' THEN 'Image_Document'
            WHEN drefc.content_attachment_content_type LIKE '%text%' THEN 'Text_Document'
            ELSE 'Document_Reference'
        END as diagnostic_category,

        -- Metadata
        dref.doc_status as test_status,
        NULL as test_orderer,
        CAST(NULL AS BIGINT) as component_count,

        -- ====================================================================
        -- NLP PRIORITIZATION FRAMEWORK (for document processing workflows)
        -- ====================================================================

        -- Extraction priority (1=highest, 5=lowest) - content and temporal based
        CASE
            -- Priority 1: Final surgical pathology reports and definitive diagnoses
            WHEN LOWER(dref.description) LIKE '%surgical%pathology%final%' THEN 1
            WHEN LOWER(dref.description) LIKE '%pathology%final%diagnosis%' THEN 1
            WHEN LOWER(dref.description) LIKE '%final%pathology%report%' THEN 1
            WHEN LOWER(dref.type_text) LIKE '%surgical pathology%' THEN 1

            -- Priority 2: Molecular pathology and genomics reports (send-outs, CLIA labs)
            WHEN LOWER(dref.description) LIKE '%molecular%pathology%' THEN 2
            WHEN LOWER(dref.description) LIKE '%genomic%' THEN 2
            WHEN LOWER(dref.description) LIKE '%genomics%' THEN 2
            WHEN LOWER(dref.description) LIKE '%ngs%' THEN 2  -- Next-generation sequencing
            WHEN LOWER(dref.description) LIKE '%clia%' THEN 2  -- CLIA certified lab reports
            WHEN LOWER(dref.description) LIKE '%pathology%consult%' THEN 2
            WHEN LOWER(dref.type_text) LIKE '%pathology%' THEN 2

            -- Priority 3: Outside/send-out pathology summaries
            WHEN LOWER(dref.description) LIKE '%outside%pathology%' THEN 3
            WHEN LOWER(dref.description) LIKE '%send%out%' THEN 3
            WHEN LOWER(dref.description) LIKE '%external%pathology%' THEN 3
            WHEN dref.type_text = 'ONC Outside Summaries' AND LOWER(dref.description) LIKE '%pathology%' THEN 3

            -- Priority 4: Biopsy and specimen reports
            WHEN LOWER(dref.description) LIKE '%biopsy%pathology%' THEN 4
            WHEN LOWER(dref.description) LIKE '%specimen%pathology%' THEN 4
            WHEN LOWER(dref.description) LIKE '%pathology%gross%' THEN 4

            -- Priority 5: Other pathology documents
            ELSE 5
        END as extraction_priority,

        -- Document category for NLP extraction type
        CASE
            WHEN LOWER(dref.description) LIKE '%final%' AND LOWER(dref.description) LIKE '%pathology%'
                THEN 'Final Pathology Report'
            WHEN LOWER(dref.description) LIKE '%surgical%pathology%'
                THEN 'Surgical Pathology Report'
            WHEN LOWER(dref.description) LIKE '%molecular%' OR LOWER(dref.description) LIKE '%genomic%'
                THEN 'Molecular Pathology Report'
            WHEN LOWER(dref.description) LIKE '%outside%' OR LOWER(dref.description) LIKE '%send%out%'
                THEN 'Outside Pathology Summary'
            WHEN LOWER(dref.description) LIKE '%biopsy%'
                THEN 'Biopsy Report'
            WHEN LOWER(dref.description) LIKE '%consult%'
                THEN 'Pathology Consultation'
            WHEN LOWER(dref.description) LIKE '%specimen%'
                THEN 'Specimen Report'
            WHEN LOWER(dref.description) LIKE '%gross%'
                THEN 'Gross Pathology Observation'
            ELSE 'Other Pathology Document'
        END as document_category,

        -- Temporal relevance to surgery (days from surgery)
        ABS(DATE_DIFF('day', ts.surgery_date, TRY(CAST(CAST(dref.date AS VARCHAR) AS DATE)))) as days_from_surgery

    FROM tumor_surgeries ts
    INNER JOIN fhir_prd_db.document_reference_context_encounter drce_filter
        ON REPLACE(drce_filter.context_encounter_reference, 'Encounter/', '') = ts.surgery_encounter
    INNER JOIN fhir_prd_db.document_reference dref
        ON dref.id = drce_filter.document_reference_id
        -- ENCOUNTER-ONLY linkage (no patient-level fallback due to performance)
        -- Captures documents explicitly linked to surgical encounters

    -- Join for document content/URL (REQUIRED for NLP processing)
    LEFT JOIN fhir_prd_db.document_reference_content drefc
        ON dref.id = drefc.document_reference_id

    -- Join for encounter linkage
    LEFT JOIN fhir_prd_db.document_reference_context_encounter drce
        ON dref.id = drce.document_reference_id

    -- LEFT JOIN to surgical_specimens (if available)
    LEFT JOIN surgical_specimens ss
        ON ts.patient_fhir_id = ss.patient_fhir_id
        AND ABS(DATE_DIFF('day',
            TRY(CAST(CAST(dref.date AS VARCHAR) AS DATE)),
            ss.surgery_date
        )) <= 7

    WHERE dref.subject_reference IS NOT NULL
      AND (dref.description IS NOT NULL OR drefc.content_attachment_url IS NOT NULL)
      -- Filter to pathology/diagnostic documents only (keyword-based)
      -- Includes: surgical pathology, biopsy reports, genomics, molecular diagnostics
      AND (
          LOWER(dref.description) LIKE '%pathology%'
          OR LOWER(dref.description) LIKE '%surgical%'
          OR LOWER(dref.description) LIKE '%biopsy%'
          OR LOWER(dref.description) LIKE '%specimen%'
          OR LOWER(dref.description) LIKE '%diagnostic%'
          OR LOWER(dref.description) LIKE '%genomic%'
          OR LOWER(dref.description) LIKE '%molecular%'
          OR LOWER(dref.type_text) LIKE '%pathology%'
          OR LOWER(dref.type_text) LIKE '%surgical%'
          OR LOWER(dref.type_text) LIKE '%diagnostic%'
      )
      -- Removed catch-all: drefc.content_attachment_url IS NOT NULL
      -- Was adding 3M+ non-pathology documents (radiology, notes, etc.)
),

-- ============================================================================
-- CTE 7: Problem List Diagnoses (patient-level ICD-10/SNOMED codes)
-- ============================================================================
problem_list_diagnoses AS (
    SELECT
        ts.patient_fhir_id,
        'problem_list_diagnosis' as diagnostic_source,
        c.id as source_id,
        date_parse(c.onset_date_time, '%Y-%m-%dT%H:%i:%sZ') as diagnostic_datetime,
        TRY(date_parse(CAST(c.onset_date_time AS VARCHAR), '%Y-%m-%d')) as diagnostic_date,

        -- Diagnosis identification
        c.code_text as diagnostic_name,
        cc.code_coding_code as code,  -- ICD-10 or SNOMED code
        NULL as coding_system_code,
        cc.code_coding_system as coding_system_name,  -- Will be ICD-10 or SNOMED URI

        -- Component (condition category)
        COALESCE(ccat.category_text, 'Unknown') as component_name,

        -- Result value (combine code display and verification status)
        CASE
            WHEN cc.code_coding_display IS NOT NULL AND c.verification_status_text IS NOT NULL
                THEN cc.code_coding_display || ' | Status: ' || c.verification_status_text
            WHEN cc.code_coding_display IS NOT NULL
                THEN cc.code_coding_display
            ELSE c.code_text
        END as result_value,

        -- Test metadata
        'Problem List' as test_lab,

        -- No specimen for problem list diagnoses (patient-level, not specimen-level)
        NULL as specimen_types,
        NULL as specimen_sites,
        NULL as specimen_collection_datetime,

        -- Procedure linkage (to surgery)
        ts.procedure_fhir_id as linked_procedure_id,
        ts.surgery_name as linked_procedure_name,
        ts.surgery_datetime as linked_procedure_datetime,

        -- Encounter linkage
        REPLACE(c.encounter_reference, 'Encounter/', '') as encounter_id,

        -- Generic categorization based on ICD-10/SNOMED code
        CASE
            WHEN cc.code_coding_system LIKE '%icd-10%' THEN 'ICD10_Diagnosis'
            WHEN cc.code_coding_system LIKE '%snomed%' THEN 'SNOMED_Diagnosis'
            WHEN ccat.category_text = 'problem-list-item' THEN 'Problem_List_Item'
            WHEN ccat.category_text = 'encounter-diagnosis' THEN 'Encounter_Diagnosis'
            ELSE 'Clinical_Diagnosis'
        END as diagnostic_category,

        -- Metadata
        c.clinical_status_text as test_status,
        c.recorder_display as test_orderer,
        CAST(NULL AS BIGINT) as component_count,

        -- NLP prioritization (problem list diagnoses are structured codes - no prioritization)
        CAST(NULL AS INTEGER) as extraction_priority,
        CAST(NULL AS VARCHAR) as document_category,
        ABS(DATE_DIFF('day', ts.surgery_date, TRY(CAST(CAST(c.onset_date_time AS VARCHAR) AS DATE)))) as days_from_surgery

    FROM tumor_surgeries ts
    INNER JOIN fhir_prd_db.condition c
        ON ts.patient_fhir_id = REPLACE(c.subject_reference, 'Patient/', '')
        -- Link diagnoses within ±180 days of surgery (may predate or follow surgery)
        AND ABS(DATE_DIFF('day', ts.surgery_date, TRY(CAST(CAST(c.onset_date_time AS VARCHAR) AS DATE)))) <= 180

    -- Join for ICD-10/SNOMED codes
    LEFT JOIN fhir_prd_db.condition_code_coding cc
        ON c.id = cc.condition_id

    -- Join for condition category (problem-list-item vs encounter-diagnosis)
    LEFT JOIN fhir_prd_db.condition_category ccat
        ON c.id = ccat.condition_id

    WHERE c.subject_reference IS NOT NULL
      AND c.id IS NOT NULL

      -- Filter to CNS/brain tumor-related diagnoses
      AND (
          -- ICD-10 C-codes for brain tumors (C70-C72, C79.3x)
          cc.code_coding_code LIKE 'C70%'
          OR cc.code_coding_code LIKE 'C71%'
          OR cc.code_coding_code LIKE 'C72%'
          OR cc.code_coding_code LIKE 'C79.3%'

          -- ICD-10 D-codes for benign brain tumors (D32-D33, D43)
          OR cc.code_coding_code LIKE 'D32%'
          OR cc.code_coding_code LIKE 'D33%'
          OR cc.code_coding_code LIKE 'D43%'

          -- SNOMED codes for brain/CNS neoplasms
          OR LOWER(c.code_text) LIKE '%brain%tumor%'
          OR LOWER(c.code_text) LIKE '%brain%neoplasm%'
          OR LOWER(c.code_text) LIKE '%glioma%'
          OR LOWER(c.code_text) LIKE '%glioblastoma%'
          OR LOWER(c.code_text) LIKE '%medulloblastoma%'
          OR LOWER(c.code_text) LIKE '%ependymoma%'
          OR LOWER(c.code_text) LIKE '%astrocytoma%'
          OR LOWER(c.code_text) LIKE '%oligodendroglioma%'
          OR LOWER(c.code_text) LIKE '%craniopharyngioma%'
          OR LOWER(c.code_text) LIKE '%meningioma%'
          OR LOWER(c.code_text) LIKE '%cns%tumor%'
          OR LOWER(c.code_text) LIKE '%central nervous system%neoplasm%'
      )
),

-- ============================================================================
-- CTE 6: Unified Pathology Diagnostics
-- ============================================================================
-- NOTE: Removed molecular_diagnostics to avoid duplication with observations.
--       Use v_molecular_tests as a separate curated molecular testing view.
-- ============================================================================
unified_diagnostics AS (
    SELECT * FROM surgical_pathology_observations
    UNION ALL
    SELECT * FROM surgical_pathology_narratives
    UNION ALL
    SELECT * FROM pathology_document_references
    UNION ALL
    SELECT * FROM problem_list_diagnoses
)

-- ============================================================================
-- FINAL SELECT: All surgery-linked pathology diagnostics
-- ============================================================================
SELECT
    ud.patient_fhir_id,
    ud.diagnostic_source,
    ud.source_id,
    ud.diagnostic_datetime,
    ud.diagnostic_date,

    -- Diagnostic details (preserved as-is, no interpretation)
    ud.diagnostic_name,
    ud.code,
    ud.coding_system_code,
    ud.coding_system_name,
    ud.component_name,
    ud.result_value,  -- Raw result value for downstream analysis
    ud.diagnostic_category,  -- Generic category based on FHIR resource type

    -- Specimen details
    ud.specimen_types,
    ud.specimen_sites,
    ud.specimen_collection_datetime,

    -- Test metadata
    ud.test_lab,
    ud.test_status,
    ud.test_orderer,
    ud.component_count,

    -- Procedure linkage
    ud.linked_procedure_id,
    ud.linked_procedure_name,
    ud.linked_procedure_datetime,
    TRY(DATE_DIFF('day', CAST(ud.linked_procedure_datetime AS DATE), ud.diagnostic_date)) as days_from_procedure,

    -- Encounter
    ud.encounter_id,

    -- ========================================================================
    -- NLP PRIORITIZATION FIELDS (for document processing workflows)
    -- ========================================================================
    -- extraction_priority: 1-5 scale for NLP targeting (1=highest value)
    --   Priority 1: Final pathology reports, definitive diagnoses
    --   Priority 2: Molecular/genomics reports, surgical pathology reports
    --   Priority 3: Outside summaries, send-out reports
    --   Priority 4: Biopsy/specimen reports, consultations
    --   Priority 5: Other pathology documents
    --   NULL: Structured data (observations, conditions) - no NLP needed
    --
    -- document_category: Human-readable document type for extraction workflows
    --   Categories: Final Pathology Report, Surgical Pathology Report,
    --              Molecular Pathology Report, Outside Pathology Summary,
    --              Biopsy Report, Pathology Consultation, etc.
    --   NULL: Structured data sources
    --
    -- days_from_surgery: Temporal relevance metric for prioritization
    --   Closer to surgery date = higher clinical relevance
    --   Used to prioritize documents when multiple exist at same priority level
    -- ========================================================================
    ud.extraction_priority,
    ud.document_category,
    ud.days_from_surgery

FROM unified_diagnostics ud

WHERE ud.patient_fhir_id IS NOT NULL

ORDER BY ud.patient_fhir_id, ud.diagnostic_datetime, ud.diagnostic_category;