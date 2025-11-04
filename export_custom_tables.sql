/*
  Exported: 2025-11-03T13:13:48Z
  Name: export_custom_tables
  ID: 60807164-59d6-46aa-b2d1-578e4fd499b7
  Database: fhir_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/60807164-59d6-46aa-b2d1-578e4fd499b7
  Description: 
*/
select * from patient_access;

select * from cbtn_enrolled_patients
where organization='The Children''s Hospital of Philadelphia';

select * from patient_medications;

select * from lab_tests;

select * from lab_test_results;

select * from radiology_imaging;

select * from radiology_imaging_mri;

select * from radiology_imaging_mri_results;

select * from problem_list_diagnoses;

select * from molecular_tests;

select * from molecular_test_results;
