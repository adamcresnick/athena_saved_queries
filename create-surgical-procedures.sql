/*
  Exported: 2025-11-03T13:13:48Z
  Name: create-surgical-procedures
  ID: 39fe6ab1-4134-4f5c-a8f7-809f4b23545a
  Database: fhir_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/39fe6ab1-4134-4f5c-a8f7-809f4b23545a
  Description: 
*/
select subject_reference as patient_id,
patient_access.mrn,
procedure.id as procedure_id,
identifier_value as epic_case_orlog_id,
procedure_code_coding.code_coding_code as cpt_code,
procedure.code_text as procedure_display,
status,
performed_period_start,
performed_period_end,
procedure_performer.performer_actor_display as performer,
outcome_text
from procedure
left join procedure_category_coding on procedure.id=procedure_category_coding.procedure_id
left join procedure_code_coding on procedure.id=procedure_code_coding.procedure_id and code_coding_system='http://www.ama-assn.org/go/cpt'
left join procedure_identifier on procedure.id=procedure_identifier.procedure_id and identifier_type_text='ORL'
left join procedure_performer on procedure.id=procedure_performer.procedure_id
inner join patient_access on procedure.subject_reference=patient_access.id
where category_coding_code='387713003'
and category_text <>'Surgical History'
and status='completed'
