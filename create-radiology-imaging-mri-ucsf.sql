/*
  Exported: 2025-11-03T13:13:48Z
  Name: create-radiology-imaging-mri-ucsf
  ID: c7c4b09c-cf52-4ebc-b7ab-dfefe8e23e77
  Database: fhir_v1_ucsf_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/c7c4b09c-cf52-4ebc-b7ab-dfefe8e23e77
  Description: this version of the query does not utilize the patient_access table
*/
--drop table if exists radiology_imaging_mri;
--select * from radiology_imaging_mri;

create table radiology_imaging_mri as (select service_request.subject_reference as patient_id,
patient.identifier_mrn,
service_request.id as imaging_procedure_id,
diagnostic_report.id as result_diagnostic_report_id,
diagnostic_report.effective_date_time result_datetime,  
authored_on, 
service_request.code_text as imaging_procedure, 
service_request.status as imaging_procedure_status,
service_request.requester_display as imaging_requester
from service_request
left join service_request_category on service_request.id=service_request_category.service_request_id
inner join diagnostic_report_based_on drbo on service_request.id=drbo.based_on_reference --only include service requests that have results returned for them. this filters out all the ancillary request types created by fhir and matches what would be seen in the EHR
left join diagnostic_report on drbo.diagnostic_report_id=diagnostic_report.id
inner join patient on service_request.subject_reference=patient.id
where 
service_request.status<>'revoked'
and service_request_category.category_text='Imaging'
and lower(service_request.code_text) like 'mr%'
order by patient_id, service_request.authored_on desc)
