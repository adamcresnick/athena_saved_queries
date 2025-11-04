/*
  Exported: 2025-11-03T13:13:48Z
  Name: get-radiology-reports-for-brim
  ID: e9215679-1e7f-411c-8c8a-06c1f86ad241
  Database: fhir_v2_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/e9215679-1e7f-411c-8c8a-06c1f86ad241
  Description: 
*/
select 
mri.mrn as PERSON_ID,
mri.accession_number as ACCESSION_NUM,
mri.imaging_procedure as NOTE_TITLE, 
mri.result_datetime as NOTE_DATETIME, 
ob.value_string as NOTE_TEXT,
ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS NOTE_ID
from radiology_imaging_mri mri
left join diagnostic_report_result drr on mri.result_diagnostic_report_id=drr.diagnostic_report_id
left join fhir_v1_prd_db.observation ob on drr.result_reference=ob.id
where result_display in ('Narrative') and
    (lower(mri.imaging_procedure) like '%brain%'
    OR lower(mri.imaging_procedure) like '%neuro%'
    OR lower(mri.imaging_procedure) like '%head%'
    OR lower(mri.imaging_procedure) like '%spine%'
    OR lower(mri.imaging_procedure) like '%vertebral%'
    OR lower(mri.imaging_procedure) like '%neck%')
