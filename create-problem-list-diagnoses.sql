/*
  Exported: 2025-11-03T13:13:48Z
  Name: create-problem-list-diagnoses
  ID: cdce4c89-425a-4bde-bc5b-ebe09d239657
  Database: fhir_v2_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/cdce4c89-425a-4bde-bc5b-ebe09d239657
  Description: 
*/
--drop table if exists problem_list_diagnoses;
--select * from problem_list_diagnoses;


create table problem_list_diagnoses as (
select condition.subject_reference as patient_id,
patient_access.mrn,
condition.id as condition_id,
code_text  as diagnosis_name, 
clinical_status_text,
onset_date_time,
abatement_date_time,
recorded_date,
icd10.code_coding_code as icd10_code,
icd10.code_coding_display as icd10_display,
snomed.code_coding_code as snomed_code,
snomed.code_coding_display as snomed_display
from condition
inner join patient_access on condition.subject_reference=patient_access.id
left join condition_category on condition.id=condition_category.condition_id
left join condition_code_coding as icd10 on condition.id=icd10.condition_id 
    and icd10.code_coding_system='http://hl7.org/fhir/sid/icd-10-cm'
left join condition_code_coding as snomed on condition.id=snomed.condition_id 
    and snomed.code_coding_system='http://snomed.info/sct'
where condition_category.category_text='Problem List Item'
)
