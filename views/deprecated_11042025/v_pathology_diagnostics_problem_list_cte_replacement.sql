-- ============================================================================
-- REPLACEMENT CTE for problem_list_diagnoses in v_pathology_diagnostics.sql
-- ============================================================================
-- This replaces lines 485-599 in the original file
-- Uses v_problem_list_diagnoses view + curated pediatric CNS SNOMED codes
-- ============================================================================

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
        NULL as encounter_id,

        -- Diagnostic categorization
        CASE
            WHEN vpld.icd10_code IS NOT NULL THEN 'ICD10_Diagnosis'
            WHEN vpld.snomed_code IS NOT NULL THEN 'SNOMED_Diagnosis'
            ELSE 'Clinical_Diagnosis'
        END as diagnostic_category,

        -- Metadata
        vpld.clinical_status_text as test_status,
        NULL as test_orderer,
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
)
