/*
  Exported: 2025-11-03T13:13:48Z
  Name: create-radiology-imaging-table
  ID: 1032c626-965b-4262-9875-c275bee5d17d
  Database: fhir_v2_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/1032c626-965b-4262-9875-c275bee5d17d
  Description: 
*/
--drop table if exists radiology_imaging;
--select * from radiology_imaging;


create table radiology_imaging as (
select distinct
service_request.subject_reference as patient_id,
patient_access.mrn,
service_request.id as imaging_procedure_id,
diagnostic_report.id as result_diagnostic_report_id,
diagnostic_report.effective_date_time result_datetime,  
authored_on, 
service_request_identifier.identifier_value as accession_number,
service_request.code_text as imaging_procedure, 
service_request.status as imaging_procedure_status,
service_request.requester_display as imaging_requester
from service_request
left join service_request_category on service_request.id=service_request_category.service_request_id
inner join diagnostic_report_based_on drbo on service_request.id=drbo.based_on_reference --only include service requests that have results returned for them. this filters out all the ancillary request types created by fhir and matches what would be seen in the EHR
left join diagnostic_report on drbo.diagnostic_report_id=diagnostic_report.id
inner join patient_access on service_request.subject_reference=patient_access.id
left join service_request_identifier on service_request.id=service_request_identifier.service_request_id 
    and service_request_identifier.identifier_type_text='Filler Identifier'
where 
service_request.status<>'revoked'
and service_request_category.category_text='Imaging'
order by patient_id, service_request.authored_on desc)
