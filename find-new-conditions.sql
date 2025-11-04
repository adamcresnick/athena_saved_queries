/*
  Exported: 2025-11-03T13:13:48Z
  Name: find-new-conditions
  ID: e9e71566-5162-40bd-880a-1179a22bdcad
  Database: fhir_chop_inc_2025_10_23_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/e9e71566-5162-40bd-880a-1179a22bdcad
  Description: 
*/
select '21st', count(distinct condition_id) conditions, count(distinct mrn) participants
from fhir_cbtn_inc_2025_10_21_prd_db.problem_list_diagnoses
where condition_id not in (select condition_id from fhir_prd_db.problem_list_diagnoses)

union

select '22nd', count(distinct condition_id), count(distinct mrn)
from fhir_cbtn_inc_2025_10_22_prd_db.problem_list_diagnoses
where condition_id not in (select condition_id from fhir_prd_db.problem_list_diagnoses
                            union select condition_id from fhir_cbtn_inc_2025_10_21_prd_db.problem_list_diagnoses)
                            
union

select '23rd', count(distinct condition_id), count(distinct mrn)
from fhir_chop_inc_2025_10_23_prd_db.problem_list_diagnoses
where condition_id not in (select condition_id from fhir_prd_db.problem_list_diagnoses
                            union select condition_id from fhir_cbtn_inc_2025_10_21_prd_db.problem_list_diagnoses
                            union select condition_id from fhir_cbtn_inc_2025_10_22_prd_db.problem_list_diagnoses)
order by 1
