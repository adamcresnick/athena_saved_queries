/*
  Exported: 2025-11-03T13:13:48Z
  Name: create-mri-results
  ID: 33d6ddfd-81dc-4f00-bf53-1e8615fd00bd
  Database: fhir_v2_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/33d6ddfd-81dc-4f00-bf53-1e8615fd00bd
  Description: 
*/
--drop table if exists radiology_imaging_mri_results;
--select * from radiology_imaging_mri_results;

create table radiology_imaging_mri_results as(
select 
mri.patient_id,
mri.mrn, 
mri.imaging_procedure, 
mri.result_datetime, 
mri.accession_number,
result_display, 
ob.value_string,
mri.imaging_procedure_id,
mri.result_diagnostic_report_id,
ob.id result_observation_id
from radiology_imaging_mri mri
left join diagnostic_report_result drr on mri.result_diagnostic_report_id=drr.diagnostic_report_id
left join observation ob on drr.result_reference=ob.id
)
