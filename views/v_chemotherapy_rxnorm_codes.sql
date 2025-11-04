-- Comprehensive Chemotherapy RxNorm Product→Ingredient Mapping View
-- Generated from RADIANT Unified Chemotherapy Index
-- Total mappings: 2804
-- Product codes: 2337
-- Ingredient codes: 412
-- Source: /Users/resnick/Downloads/RADIANT_Portal/RADIANT_PCA/unified_chemo_index/rxnorm_code_map.csv
-- 
-- CRITICAL: This view enables matching medications coded at the PRODUCT level
-- (e.g., brand names, formulations) to their ingredient-level chemotherapy drugs.
-- Without this mapping, we would miss many medications in FHIR data!

CREATE OR REPLACE VIEW fhir_prd_db.v_chemotherapy_rxnorm_codes AS
SELECT code_cui AS product_rxnorm_code, code_tty AS term_type, ingredient_rxcui AS ingredient_rxnorm_code, ingredient_drug_id
FROM (VALUES...)