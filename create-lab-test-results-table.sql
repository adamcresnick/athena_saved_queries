/*
  Exported: 2025-11-03T13:13:48Z
  Name: create-lab-test-results-table
  ID: db73645d-e349-4803-b218-f4ad411f40ce
  Database: fhir_v2_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/db73645d-e349-4803-b218-f4ad411f40ce
  Description: 
*/
create table lab_test_results as (select 
service_request.subject_reference as patient_id,
patient_access.mrn,
service_request.id as test_id,
diagnostic_report.id as result_diagnostic_report_id,
diagnostic_report.effective_date_time result_datetime,  
authored_on, 
service_request.code_text as lab_test_name, 
service_request.status as lab_test_status,
diagnostic_report_result.result_display test_component,
observation.value_quantity_value,
observation.value_quantity_unit,
observation.value_quantity_code,
observation.value_quantity_system,
observation.value_codeable_concept_text,
observation.value_range_low_value,
observation.value_range_low_unit,
observation.value_range_high_value,
observation.value_range_high_unit,
observation.value_ratio_numerator_value,
observation.value_ratio_numerator_unit,
observation.value_ratio_numerator_code,
observation.value_ratio_denominator_value,
observation.value_ratio_denominator_unit,
observation.value_ratio_denominator_code,
observation.value_string,
observation.value_boolean,
observation.value_integer,
service_request.requester_display as lab_test_requester
from service_request
left join service_request_category on service_request.id=service_request_category.service_request_id
inner join diagnostic_report_based_on drbo on service_request.id=drbo.based_on_reference --only include service requests that have results returned for them. this filters out all the ancillary request types created by fhir and matches what would be seen in the EHR
left join diagnostic_report on drbo.diagnostic_report_id=diagnostic_report.id
left join diagnostic_report_result on diagnostic_report.id=diagnostic_report_result.diagnostic_report_id
left join observation on diagnostic_report_result.result_reference=observation.id 
inner join patient_access on service_request.subject_reference=patient_access.id
where 
service_request.status<>'revoked'
and service_request_category.category_text='Lab'
order by service_request.subject_reference,diagnostic_report.effective_date_time, diagnostic_report.id
)
