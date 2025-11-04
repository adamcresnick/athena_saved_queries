/*
  Exported: 2025-11-03T13:13:48Z
  Name: create-patient-medications
  ID: 2da81d22-3224-4cb1-9582-ab4e5f2ea1ad
  Database: fhir_v2_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/2da81d22-3224-4cb1-9582-ab4e5f2ea1ad
  Description: 
*/
--drop table if exists patient_medications;
--select * from patient_medications;

create table patient_medications as(
with rxnorms as (
select medication_id, listagg(distinct code_coding_code, '; ') WITHIN GROUP (ORDER BY code_coding_code) rx_norm_codes
from medication_code_coding
where code_coding_system='http://www.nlm.nih.gov/research/umls/rxnorm'
group by medication_id
)

select  
subject_reference as patient_id, 
med_req.id as medication_request_id, 
medication_reference_display as medication_name, 
med.form_text,
medication_reference_reference as medication_id, 
rx_norm_codes,
authored_on, 
requester_display as requester_name, 
med_req.status, 
encounter_display
from medication_request med_req 
left join medication med on med_req.medication_reference_reference=med.id
left join rxnorms rx on med.id=rx.medication_id
where med_req.status not in ('cancelled','on-hold','draft') 
order by med_req.authored_on desc
)
