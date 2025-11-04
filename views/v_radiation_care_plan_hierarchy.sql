CREATE OR REPLACE VIEW fhir_prd_db.v_radiation_care_plan_hierarchy AS
SELECT
    cp.subject_reference as patient_fhir_id,
    cppo.care_plan_id,
    cppo.part_of_reference as cppo_part_of_reference,
    cp.status as cp_status,
    cp.intent as cp_intent,
    cp.title as cp_title,
    COALESCE(
        TRY(date_parse(cp.period_start, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(cp.period_start, '%Y-%m-%d'))
    ) as cp_period_start,
    TRY(date_parse(cp.period_end, '%Y-%m-%dT%H:%i:%sZ')) as cp_period_end
FROM fhir_prd_db.care_plan_part_of cppo
INNER JOIN fhir_prd_db.care_plan cp ON cppo.care_plan_id = cp.id
WHERE cp.subject_reference IS NOT NULL
  AND (LOWER(cp.title) LIKE '%radiation%'
       OR LOWER(cp.title) LIKE '%rad%onc%'
       OR LOWER(cp.title) LIKE '%radiotherapy%'
       OR LOWER(cp.title) LIKE '%imrt%'
       OR LOWER(cp.title) LIKE '%proton%'
       OR LOWER(cp.title) LIKE '%cyberknife%'
       OR LOWER(cp.title) LIKE '%gamma knife%'
       OR LOWER(cp.title) LIKE '%xrt%'
       OR LOWER(cppo.part_of_reference) LIKE '%radiation%'
       OR LOWER(cppo.part_of_reference) LIKE '%rad%onc%'
       OR LOWER(cppo.part_of_reference) LIKE '%radiotherapy%')
ORDER BY cp.period_start;