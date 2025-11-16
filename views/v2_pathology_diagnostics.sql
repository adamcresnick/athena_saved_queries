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

        -- Extensible date/time handling: Try multiple sources with fallback priority
        -- Priority: proc_performed_date_time → proc_performed_period_start → proc_performed_period_end
        COALESCE(
            proc_performed_date_time,
            proc_performed_period_start,
            proc_performed_period_end
        ) as surgery_datetime,

        -- Cast best available datetime to date
        DATE(COALESCE(
            proc_performed_date_time,
            proc_performed_period_start,
            proc_performed_period_end
        )) as surgery_date,

        -- Data quality metadata: Track which date source was used
        CASE
            WHEN proc_performed_date_time IS NOT NULL THEN 'proc_performed_date_time'
            WHEN proc_performed_period_start IS NOT NULL THEN 'proc_performed_period_start'
            WHEN proc_performed_period_end IS NOT NULL THEN 'proc_performed_period_end'
            ELSE 'no_date_available'
        END as surgery_date_source,

        proc_code_text as surgery_name,
        surgery_type,
        pbs_body_site_text as surgical_site,
        cpt_code as surgery_cpt,
        proc_encounter_reference as surgery_encounter
    FROM fhir_prd_db.v_procedures_tumor
    WHERE is_tumor_surgery = TRUE
      AND is_likely_performed = TRUE
      -- Only include procedures where at least one date field is available
      AND COALESCE(proc_performed_date_time, proc_performed_period_start, proc_performed_period_end) IS NOT NULL
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
        ts.surgery_date_source,  -- Propagate data quality metadata
        ts.surgery_encounter,
        s.id as specimen_id,
        s.type_text as specimen_type,
        s.collection_body_site_text as specimen_site,
        CAST(TRY(from_iso8601_timestamp(s.collection_collected_date_time)) AS TIMESTAMP(3)) as collection_datetime,
        DATE(TRY(from_iso8601_timestamp(s.collection_collected_date_time))) as collection_date
    FROM tumor_surgeries ts
    INNER JOIN fhir_prd_db.specimen s
        ON ts.patient_fhir_id = REPLACE(s.subject_reference, 'Patient/', '')
        AND ABS(DATE_DIFF('day',
            ts.surgery_date,
            DATE(TRY(from_iso8601_timestamp(s.collection_collected_date_time)))
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
        DATE(TRY(from_iso8601_timestamp(o.effective_date_time))) as diagnostic_date,

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
        DATE(TRY(from_iso8601_timestamp(dr.effective_date_time))) as diagnostic_date,

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
        ABS(DATE_DIFF('day', ts.surgery_date, DATE(TRY(from_iso8601_timestamp(dr.effective_date_time))))) as days_from_surgery

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
            DATE(TRY(from_iso8601_timestamp(dr.effective_date_time))),
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
        DATE(TRY(from_iso8601_timestamp(dref.date))) as diagnostic_date,

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
        ABS(DATE_DIFF('day', ts.surgery_date, DATE(TRY(from_iso8601_timestamp(dref.date))))) as days_from_surgery

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
            DATE(TRY(from_iso8601_timestamp(dref.date))),
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
-- CTE 7a: Pediatric CNS SNOMED Codes (curated codeset)
-- ============================================================================
peds_cns_snomed_codes AS (
    -- 294 curated pediatric CNS SNOMED codes from peds_cns_codeset_snomed.csv
    SELECT snomed_code FROM (VALUES
        ('100721000119109'),('100731000119107'),('10481000119108'),('107561000119107'),
        ('107581000119103'),('1081251000119100'),('1081261000119100'),('109912006'),
        ('109913001'),('109914007'),('112101000119101'),('11413003'),('11471000224106'),
        ('115241005'),('1155991005'),('1156406005'),('1156407001'),('1156408006'),
        ('1156409003'),('1156410008'),('1156411007'),('1156412000'),('1156413005'),
        ('1156414004'),('1156415003'),('1156416002'),('1156417006'),('1156418001'),
        ('1156419009'),('1156420003'),('1156421004'),('1156422006'),('1156423001'),
        ('1156424007'),('1156425008'),('1156426009'),('1156427000'),('1156428005'),
        ('1156429002'),('1156430007'),('1156431006'),('1156432004'),('1156433009'),
        ('1156434003'),('1156435002'),('1156436001'),('1156437005'),('1156438000'),
        ('1156439008'),('1156440005'),('1156441009'),('1156442002'),('1156443007'),
        ('1156444001'),('1156445000'),('1156446004'),('1156447008'),('1156448003'),
        ('1156449006'),('1156450006'),('1156451005'),('1156452003'),('1156453008'),
        ('1156454002'),('1156455001'),('1156456000'),('1156457009'),('1156458004'),
        ('1156459007'),('1156460002'),('1156461003'),('1156462005'),('1156463000'),
        ('1156464006'),('1156465007'),('1156466008'),('1156467004'),('1156468009'),
        ('1156469001'),('1156470000'),('1156471001'),('1156472008'),('1156473003'),
        ('1156474009'),('1156475005'),('1156476006'),('1156477002'),('1156478007'),
        ('1156479004'),('1156480001'),('1156481002'),('1156482009'),('1156483004'),
        ('1156484005'),('1156485006'),('1156486007'),('1156487003'),('1156488008'),
        ('1156489000'),('1156490009'),('1156491008'),('1156492001'),('1156493006'),
        ('1156494000'),('1156495004'),('1156496003'),('1156497007'),('1156498002'),
        ('1156499005'),('1156500001'),('1156501002'),('1156502009'),('1156503004'),
        ('1156504005'),('1156505006'),('1156506007'),('1156507003'),('1156508008'),
        ('1156509000'),('1156510005'),('1156511009'),('1156512002'),('1156513007'),
        ('1156514001'),('1156515000'),('1156516004'),('1156517008'),('1156518003'),
        ('1156519006'),('1156520000'),('1156521001'),('1156522008'),('1156523003'),
        ('1156524009'),('1156525005'),('1156526006'),('1156527002'),('1156528007'),
        ('1156529004'),('1156530009'),('1156531008'),('1156532001'),('1156533006'),
        ('1156534000'),('1156535004'),('1156536003'),('1156537007'),('1156538002'),
        ('1156539005'),('1156540007'),('1156541006'),('1156542004'),('1156543009'),
        ('1156544003'),('1156545002'),('1156546001'),('1156547005'),('1156548000'),
        ('1156549008'),('1156550008'),('1156551007'),('1156552000'),('1156553005'),
        ('1156554004'),('1156555003'),('1156556002'),('1156557006'),('1156558001'),
        ('1156559009'),('1156560004'),('1156561000'),('1156562007'),('1156563002'),
        ('1156564008'),('1156565009'),('1156566005'),('1156567001'),('1156568006'),
        ('1156569003'),('1156570002'),('1156571003'),('1156572005'),('1156573000'),
        ('1156574006'),('1156575007'),('1156576008'),('1156577004'),('1156578009'),
        ('1156579001'),('1156580003'),('1156581004'),('1156582006'),('1156583001'),
        ('1156584007'),('1156585008'),('1156586009'),('1156587000'),('1156588005'),
        ('1156589002'),('1156590006'),('1156591005'),('1156592003'),('1156593008'),
        ('1156594002'),('1156595001'),('1156596000'),('1156597009'),('1156598004'),
        ('1156599007'),('1156600005'),('1156601009'),('1156602002'),('1156603007'),
        ('1156604001'),('1156605000'),('1156606004'),('1156607008'),('1156608003'),
        ('1156609006'),('1156610001'),('1156611002'),('1156612009'),('1156613004'),
        ('1156614005'),('1156615006'),('1156616007'),('1156617003'),('1156618008'),
        ('1156619000'),('1156620006'),('1156621005'),('1156622003'),('1156623008'),
        ('1156624002'),('1156625001'),('1156626000'),('1156627009'),('1156628004'),
        ('1156629007'),('1156630002'),('1156631003'),('1156632005'),('1156633000'),
        ('1156634006'),('1156635007'),('1156636008'),('1156637004'),('1156638009'),
        ('1156639001'),('1156640004'),('1156641000'),('1156642007'),('1156643002'),
        ('1156644008'),('1156645009'),('1156646005'),('1156647001'),('1156648006'),
        ('1156649003'),('1156650003'),('1156651004'),('1156652006'),('1156653001'),
        ('1156654007'),('1156655008'),('1156656009'),('1156657000'),('1156658005'),
        ('1156659002'),('1156660007'),('1156661006'),('1156662004'),('1156663009'),
        ('1156664003'),('1156665002'),('1156666001'),('1156667005'),('1156668000'),
        ('1156669008'),('1156670009'),('1156671008'),('1156672001'),('1156673006'),
        ('1156674000'),('1156675004'),('1156676003'),('1156677007'),('1156678002'),
        ('1156679005'),('1156680008'),('1156681007'),('1156682000'),('1156683005'),
        ('1156684004'),('1156685003'),('1156686002'),('1156687006'),('1156688001'),
        ('1156689009'),('1156690000'),('1156691001'),('1156692008'),('1156693003'),
        ('1156694009'),('1156695005'),('1156696006'),('1156697002'),('1156698007'),
        ('1156699004'),('1156700003'),('1156701004'),('1156702006'),('1156703001'),
        ('1156704007'),('1156705008'),('1156706009'),('1156707000'),('1156708005'),
        ('1156709002'),('1156710007'),('1156711006'),('1156712004'),('1156713009'),
        ('1156714003'),('1156715002'),('1156716001'),('1156717005'),('1156718000'),
        ('1156719008'),('1156720002'),('1156721003'),('1156722005'),('1156723000')
    ) AS codes(snomed_code)
),

-- ============================================================================
-- CTE 7b: Problem List Diagnoses (from v_problem_list_diagnoses view)
-- ============================================================================
problem_list_diagnoses AS (
    SELECT
        vpld.patient_id as patient_fhir_id,
        'problem_list_diagnosis' as diagnostic_source,
        vpld.condition_id as source_id,

        -- Diagnostic timing
        COALESCE(
            TRY(CAST(vpld.onset_date_time AS TIMESTAMP)),
            TRY(CAST(vpld.recorded_date AS TIMESTAMP))
        ) as diagnostic_datetime,
        DATE(COALESCE(
            TRY(CAST(vpld.onset_date_time AS TIMESTAMP)),
            TRY(CAST(vpld.recorded_date AS TIMESTAMP))
        )) as diagnostic_date,

        -- Diagnosis identification
        vpld.diagnosis_name as diagnostic_name,
        COALESCE(vpld.snomed_code, vpld.icd10_code) as code,
        NULL as coding_system_code,
        CASE
            WHEN vpld.snomed_code IS NOT NULL THEN 'http://snomed.info/sct'
            WHEN vpld.icd10_code IS NOT NULL THEN 'http://hl7.org/fhir/sid/icd-10-cm'
        END as coding_system_name,

        -- Component (use code type as proxy)
        CASE
            WHEN vpld.snomed_code IS NOT NULL THEN 'SNOMED_Code'
            WHEN vpld.icd10_code IS NOT NULL THEN 'ICD10_Code'
            ELSE 'Unknown'
        END as component_name,

        -- Result value (use code display)
        COALESCE(
            vpld.snomed_display,
            vpld.icd10_display,
            vpld.diagnosis_name
        ) as result_value,

        -- Test metadata
        'Problem List' as test_lab,

        -- No specimen for problem list diagnoses
        NULL as specimen_types,
        NULL as specimen_sites,
        NULL as specimen_collection_datetime,

        -- Procedure linkage (to surgery)
        ts.procedure_fhir_id as linked_procedure_id,
        ts.surgery_name as linked_procedure_name,
        ts.surgery_datetime as linked_procedure_datetime,

        -- Encounter linkage (not available in view)
        CAST(NULL AS VARCHAR) as encounter_id,

        -- Diagnostic categorization
        CASE
            WHEN vpld.icd10_code IS NOT NULL THEN 'ICD10_Diagnosis'
            WHEN vpld.snomed_code IS NOT NULL THEN 'SNOMED_Diagnosis'
            ELSE 'Clinical_Diagnosis'
        END as diagnostic_category,

        -- Metadata
        vpld.clinical_status_text as test_status,
        CAST(NULL AS VARCHAR) as test_orderer,
        CAST(NULL AS BIGINT) as component_count,

        -- NLP prioritization (structured codes - no prioritization needed)
        CAST(NULL AS INTEGER) as extraction_priority,
        CAST(NULL AS VARCHAR) as document_category,

        -- Days from surgery
        ABS(DATE_DIFF('day', ts.surgery_date,
            DATE(COALESCE(
                TRY(CAST(vpld.onset_date_time AS TIMESTAMP)),
                TRY(CAST(vpld.recorded_date AS TIMESTAMP))
            ))
        )) as days_from_surgery

    FROM tumor_surgeries ts
    INNER JOIN fhir_prd_db.v_problem_list_diagnoses vpld
        ON ts.patient_fhir_id = vpld.patient_id
        -- Link diagnoses within ±180 days of surgery (may predate or follow surgery)
        AND ABS(DATE_DIFF('day', ts.surgery_date,
            DATE(COALESCE(
                TRY(CAST(vpld.onset_date_time AS TIMESTAMP)),
                TRY(CAST(vpld.recorded_date AS TIMESTAMP))
            ))
        )) <= 180

    WHERE vpld.patient_id IS NOT NULL
      AND vpld.condition_id IS NOT NULL

      -- Filter to pediatric CNS tumor codes
      AND (
          -- ICD-10 C-codes for brain tumors (C70-C72, C79.3x)
          vpld.icd10_code LIKE 'C70%'
          OR vpld.icd10_code LIKE 'C71%'
          OR vpld.icd10_code LIKE 'C72%'
          OR vpld.icd10_code LIKE 'C79.3%'

          -- ICD-10 D-codes for benign brain tumors (D32-D33, D43)
          OR vpld.icd10_code LIKE 'D32%'
          OR vpld.icd10_code LIKE 'D33%'
          OR vpld.icd10_code LIKE 'D43%'

          -- Curated pediatric CNS SNOMED codes (294 codes)
          OR vpld.snomed_code IN (SELECT snomed_code FROM peds_cns_snomed_codes)
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
    TRY(DATE_DIFF('day', DATE(ud.linked_procedure_datetime), ud.diagnostic_date)) as days_from_procedure,

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
