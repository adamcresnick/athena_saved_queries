/*
  Exported: 2025-11-03T13:13:48Z
  Name: create-lab-test-table
  ID: 5edcebd0-c17f-4b2a-b3bc-d4e749a0917f
  Database: fhir_v2_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/5edcebd0-c17f-4b2a-b3bc-d4e749a0917f
  Description: 
*/
--drop table if exists lab_tests;
--select * from lab_tests;

create table lab_tests as (select 
service_request.subject_reference as patient_id,
patient_access.mrn,
service_request.id as test_id,
diagnostic_report.id as result_diagnostic_report_id,
diagnostic_report.effective_date_time result_datetime,  
authored_on, 
service_request.code_text as lab_test_name, 
service_request.status as lab_test_status,
service_request.requester_display as lab_test_requester
from service_request
left join service_request_category on service_request.id=service_request_category.service_request_id
inner join diagnostic_report_based_on drbo on service_request.id=drbo.based_on_reference --only include service requests that have results returned for them. this filters out all the ancillary request types created by fhir and matches what would be seen in the EHR
left join diagnostic_report on drbo.diagnostic_report_id=diagnostic_report.id
inner join patient_access on service_request.subject_reference=patient_access.id
where 
service_request.status<>'revoked'
and service_request_category.category_text='Lab'
order by patient_id, service_request.authored_on desc)
