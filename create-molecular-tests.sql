/*
  Exported: 2025-11-03T13:13:48Z
  Name: create-molecular-tests
  ID: a47ad581-1fe6-4f8f-ab51-3406337ab4ff
  Database: fhir_v2_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/a47ad581-1fe6-4f8f-ab51-3406337ab4ff
  Description: 
*/
--drop table if exists molecular_tests;
--select * from molecular_tests;

create table molecular_tests as(
select service_request.subject_reference as patient_id,
patient_access.mrn,
service_request.id as test_id,
diagnostic_report.id as result_diagnostic_report_id,
diagnostic_report.effective_date_time result_datetime,  
authored_on, 
sri.identifier_value as dgd_id,
service_request.code_text as lab_test_name, 
service_request.status as lab_test_status,
service_request.requester_display as lab_test_requester
from service_request 
left join service_request_category src on service_request.id=src.service_request_id
inner join patient_access on patient_access.id=service_request.subject_reference
left join service_request_identifier sri on service_request.id=sri.service_request_id
inner join diagnostic_report_based_on drbo on service_request.id=drbo.based_on_reference --only include service requests that have results returned for them. this filters out all the ancillary request types created by fhir and matches what would be seen in the EHR
left join diagnostic_report on drbo.diagnostic_report_id=diagnostic_report.id
--left join diagnostic_report_result on diagnostic_report.id=diagnostic_report_result.diagnostic_report_id
where 
service_request.status<>'revoked'
and (sri.identifier_system='https://open.epic.com/FHIR/20/order-accession-number/Beaker'
and sri.identifier_value like '%-GD-%')
or (sri.identifier_system='urn:oid:1.2.840.114350.1.13.20.2.7.3.798268.800' and sri.identifier_value like 'DGD%')
and service_request.code_text not in ('Normal specimen for Paired Tumor/Normal - Solid Tumor Panels','Solid Tumor Panel - Normal Paired') --exclude the test for the normal pair, as results are always attached to the tumor sample

)
