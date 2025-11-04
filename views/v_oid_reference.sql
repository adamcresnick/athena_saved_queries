-- ============================================================================
-- VIEW: v_oid_reference
-- PURPOSE: Central reference for all OIDs (Object Identifiers) used in CHOP FHIR data
-- USAGE: JOIN to any table with code_coding_system or identifier_system columns
--
-- BACKGROUND:
--   OIDs uniquely identify coding systems and Epic masterfiles
--   Epic OID structure: 1.2.840.114350.1.13.<CID>.<EnvType>.7.<Type>.<ASCII>.<Item>
--   CHOP Site Code (CID): 20
--   Environment: 2 (Production)
--
-- MAINTENANCE:
--   Add new OIDs as they are discovered in FHIR data
--   Run validation queries in testing/validate_oid_usage.sql to find undocumented OIDs
--
-- REFERENCES:
--   Epic Vendor Services: https://vendorservices.epic.com/Article?docId=epicidtypes
--   OID Repository: https://oid-base.com/
--   Internal: documentation/OID_USAGE_ANALYSIS_AND_RECOMMENDATIONS.md
-- ============================================================================

CREATE OR REPLACE VIEW fhir_prd_db.v_oid_reference AS
SELECT * FROM (VALUES
    -- ========================================================================
    -- Epic CHOP Masterfiles (Site Code: 20, Environment: 2=Production)
    -- Structure: 1.2.840.114350.1.13.20.2.7.<Type>.<ASCII_Masterfile>.<Item>
    -- ASCII Encoding: 6-digit number encoding 3-letter masterfile code
    -- ========================================================================

    -- EAP: Procedures (Epic Ambulatory Procedures)
    ('urn:oid:1.2.840.114350.1.13.20.2.7.2.696580', 'Epic', 'CHOP', 'EAP', 'Procedure', '1',
     'Procedure Masterfile ID (.1)',
     'ASCII: E(69) A(65) P(80) = 696580. Primary unique ID for procedure records.',
     'procedure_code_coding', TRUE),

    -- ORD: Orders
    ('urn:oid:1.2.840.114350.1.13.20.2.7.2.798268', 'Epic', 'CHOP', 'ORD', 'Order', '1',
     'Order Masterfile ID (.1)',
     'ASCII: O(79) R(82) D(68) = 798268. Primary unique ID for order records.',
     'order tables', TRUE),

    ('urn:oid:1.2.840.114350.1.13.20.2.7.3.798268.800', 'Epic', 'CHOP', 'ORD', 'Order', '800',
     'Order External ID (800)',
     'ASCII: O(79) R(82) D(68) = 798268, Item 800 = External ID field.',
     'order tables', TRUE),

    -- ERX: Medications (Epic Prescription)
    ('urn:oid:1.2.840.114350.1.13.20.2.7.2.698288', 'Epic', 'CHOP', 'ERX', 'Medication', '1',
     'Medication Masterfile ID (.1)',
     'ASCII: E(69) R(82) X(88) = 698288. Primary unique ID for medication records.',
     'medication_request_code_coding', TRUE),

    -- ORT: Order Type (appears in our data - needs confirmation)
    ('urn:oid:1.2.840.114350.1.13.20.7.7.2.798276', 'Epic', 'CHOP', 'ORT', 'Order Type', '1',
     'Order Type Masterfile ID (.1) [VERIFY]',
     'ASCII: O(79) R(82) T(84) = 798276. Found in data, purpose needs confirmation.',
     'procedure_code_coding', FALSE),

    -- CSN: Contact Serial Number (Encounter ID)
    ('urn:oid:1.2.840.114350.1.13.20.2.7.2.678367', 'Epic', 'CHOP', 'CSN', 'Encounter', '1',
     'Contact Serial Number (.1)',
     'ASCII: C(67) S(83) N(78) = 678367. Unique encounter/visit identifier.',
     'encounter', TRUE),

    -- DEP: Department
    ('urn:oid:1.2.840.114350.1.13.20.2.7.2.686980', 'Epic', 'CHOP', 'DEP', 'Department', '1',
     'Department Masterfile ID (.1)',
     'ASCII: D(68) E(69) P(80) = 686980. Location/department identifier.',
     'location', TRUE),

    -- SER: Provider/Service User
    ('urn:oid:1.2.840.114350.1.13.20.2.7.2.837982', 'Epic', 'CHOP', 'SER', 'Provider', '1',
     'Service User ID (.1)',
     'ASCII: S(83) E(69) R(82) = 837982. Provider/clinician identifier.',
     'practitioner', TRUE),

    -- MRN: Medical Record Number
    ('urn:oid:1.2.840.114350.1.13.20.2.7.5.737384.14', 'Epic', 'CHOP', 'MRN', 'Patient', '14',
     'Medical Record Number (Item 14)',
     'Item 14 represents MRN identifier type in Epic.',
     'patient_identifier', TRUE),

    -- ========================================================================
    -- Standard Healthcare Coding Systems (Non-Epic OIDs)
    -- Managed by external organizations (AMA, NLM, WHO, etc.)
    -- ========================================================================

    -- CPT: Current Procedural Terminology (AMA)
    ('http://www.ama-assn.org/go/cpt', 'Standard', 'AMA', 'CPT', 'Procedure', NULL,
     'Current Procedural Terminology',
     'Standard procedure coding system. Used for billing and clinical documentation.',
     'procedure_code_coding', TRUE),

    -- SNOMED CT: Clinical Terminology (IHTSDO)
    ('http://snomed.info/sct', 'Standard', 'IHTSDO', 'SNOMED CT', 'Clinical', NULL,
     'SNOMED Clinical Terminology',
     'Comprehensive clinical terminology system. Used for diagnoses, procedures, findings.',
     'multiple tables', TRUE),

    -- RxNorm: Medication Names (NLM)
    ('http://www.nlm.nih.gov/research/umls/rxnorm', 'Standard', 'NLM', 'RxNorm', 'Medication', NULL,
     'RxNorm Medication Codes',
     'Normalized medication naming system. Standard for medication coding in US.',
     'medication_request_code_coding', TRUE),

    -- NDDF: National Drug Data File
    ('urn:oid:2.16.840.1.113883.6.208', 'Standard', 'FDBH', 'NDDF', 'Medication', NULL,
     'National Drug Data File',
     'Drug formulary, pricing, and clinical screening. Managed by First Databank Health.',
     'medication_request_code_coding', TRUE),

    -- ICD-10-CM: Diagnoses (WHO/CDC)
    ('http://hl7.org/fhir/sid/icd-10-cm', 'Standard', 'WHO/CDC', 'ICD-10-CM', 'Diagnosis', NULL,
     'International Classification of Diseases v10 (US Clinical Modification)',
     'Standard diagnosis coding system. Used for billing and clinical documentation.',
     'condition_code_coding', TRUE),

    -- LOINC: Laboratory Observations (Regenstrief)
    ('http://loinc.org', 'Standard', 'Regenstrief', 'LOINC', 'Laboratory', NULL,
     'Logical Observation Identifiers Names and Codes',
     'Standard for lab tests and clinical observations. Universal test codes.',
     'observation_code_coding', TRUE),

    -- CDT-2: Dental Procedures (ADA) - DISCOVERED IN PRODUCTION
    ('urn:oid:2.16.840.1.113883.6.13', 'Standard', 'ADA', 'CDT-2', 'Dental', NULL,
     'Current Dental Terminology',
     'Dental procedure codes. Used for oral/maxillofacial surgery, TMJ procedures.',
     'procedure_code_coding', TRUE),

    -- HCPCS: Healthcare Common Procedure Coding (CMS) - DISCOVERED IN PRODUCTION
    ('urn:oid:2.16.840.1.113883.6.14', 'Standard', 'CMS', 'HCPCS', 'Procedure', NULL,
     'Healthcare Common Procedure Coding System',
     'CMS billing codes for procedures, DME, ambulance, services not covered by CPT.',
     'procedure_code_coding', TRUE),

    -- HL7 v2 Identifier Types
    ('http://terminology.hl7.org/CodeSystem/v2-0203', 'Standard', 'HL7', 'v2-0203', 'Identifier', NULL,
     'HL7 v2 Identifier Type Codes',
     'Standard identifier type codes: MR (Medical Record), DL (Drivers License), SSN, etc.',
     'identifier tables', TRUE),

    -- NPI: National Provider Identifier (CMS)
    ('http://hl7.org/fhir/sid/us-npi', 'Standard', 'CMS', 'NPI', 'Provider', NULL,
     'National Provider Identifier',
     'US provider registry. Unique identifier for healthcare providers.',
     'practitioner_identifier', TRUE),

    -- CVX: Vaccine Codes (CDC)
    ('http://hl7.org/fhir/sid/cvx', 'Standard', 'CDC', 'CVX', 'Vaccine', NULL,
     'Vaccine Administered CVX Codes',
     'Standard vaccine product codes. Used for immunization tracking.',
     'immunization_code_coding', TRUE),

    -- NDC: National Drug Code (FDA)
    ('http://hl7.org/fhir/sid/ndc', 'Standard', 'FDA', 'NDC', 'Medication', NULL,
     'National Drug Code',
     'FDA drug product identifier. Package-level medication codes.',
     'medication_request_code_coding', TRUE),

    -- ========================================================================
    -- HL7 FHIR Standard URIs (Not OIDs, but commonly used identifiers)
    -- ========================================================================

    -- UCUM: Units of Measure
    ('http://unitsofmeasure.org', 'Standard', 'Regenstrief', 'UCUM', 'Units', NULL,
     'Unified Code for Units of Measure',
     'Standard units for measurements: mg, mL, kg, etc.',
     'observation_value_quantity', TRUE),

    -- RADLEX: Radiology Lexicon
    ('http://radlex.org', 'Standard', 'RSNA', 'RadLex', 'Radiology', NULL,
     'Radiology Lexicon',
     'Standardized radiology terminology. Used for imaging procedures and findings.',
     'imaging tables', TRUE)

) AS oid_data(
    oid_uri,                    -- Full OID URI (primary key, join to code_coding_system)
    oid_source,                 -- 'Epic' or 'Standard'
    issuing_organization,       -- CHOP, AMA, NLM, WHO, etc.
    masterfile_code,            -- Short code: EAP, ORD, CPT, RxNorm, etc.
    category,                   -- Procedure, Medication, Diagnosis, Laboratory, etc.
    item_number,                -- Epic item number (.1, 800, etc.) - NULL for non-Epic OIDs
    description,                -- Human-readable description
    technical_notes,            -- Implementation notes, ASCII decoding, usage guidance
    common_fhir_tables,         -- Which FHIR tables commonly use this OID
    is_verified                 -- TRUE if confirmed in production data, FALSE if needs verification
);