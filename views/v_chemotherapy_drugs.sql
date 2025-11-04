CREATE OR REPLACE VIEW fhir_prd_db.v_chemotherapy_drugs AS
SELECT
    drug_id,
    preferred_name,
    approval_status,
    is_supportive_care,
    rxnorm_in,
    ncit_code,
    normalized_key,
    sources,
    drug_category,
    drug_type,
    therapeutic_normalized
FROM fhir_prd_db.chemotherapy_drugs;