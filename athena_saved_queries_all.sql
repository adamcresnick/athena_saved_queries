/*
  Exported: 2025-11-03T13:13:48Z
  Name: create-enrollment-table-from-csv
  ID: fe6267d4-0ca2-4706-9357-353f485093ec
  Database: fhir_v2_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/fe6267d4-0ca2-4706-9357-353f485093ec
  Description: 
*/
CREATE EXTERNAL TABLE IF NOT EXISTS `fhir_prd_db`.`cbtn_enrolled_patients` (
  `mrn` varchar(50),
  `organization` varchar(50)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'serialization.format' = ',',
    'field.delim' = ','
)
LOCATION 's3://clinical-data-extract-workflows/mrn_list/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ============================== --

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

-- ============================== --

/*
  Exported: 2025-11-03T13:13:48Z
  Name: create-radiology-imaging-table-ucsf
  ID: 088ebe47-2623-4134-8271-efc12459a099
  Database: fhir_v1_ucsf_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/088ebe47-2623-4134-8271-efc12459a099
  Description: this version of the create script does not use the patient_access table
*/
--drop table if exists radiology_imaging;
--select * from radiology_imaging;


create table radiology_imaging as (select service_request.subject_reference as patient_id,
patient_access.mrn,
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
order by patient_id, service_request.authored_on desc)

-- ============================== --

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

-- ============================== --

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

-- ============================== --

/*
  Exported: 2025-11-03T13:13:48Z
  Name: create-problem-list-diagnoses
  ID: cdce4c89-425a-4bde-bc5b-ebe09d239657
  Database: fhir_v2_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/cdce4c89-425a-4bde-bc5b-ebe09d239657
  Description: 
*/
--drop table if exists problem_list_diagnoses;
--select * from problem_list_diagnoses;


create table problem_list_diagnoses as (
select condition.subject_reference as patient_id,
patient_access.mrn,
condition.id as condition_id,
code_text  as diagnosis_name, 
clinical_status_text,
onset_date_time,
abatement_date_time,
recorded_date,
icd10.code_coding_code as icd10_code,
icd10.code_coding_display as icd10_display,
snomed.code_coding_code as snomed_code,
snomed.code_coding_display as snomed_display
from condition
inner join patient_access on condition.subject_reference=patient_access.id
left join condition_category on condition.id=condition_category.condition_id
left join condition_code_coding as icd10 on condition.id=icd10.condition_id 
    and icd10.code_coding_system='http://hl7.org/fhir/sid/icd-10-cm'
left join condition_code_coding as snomed on condition.id=snomed.condition_id 
    and snomed.code_coding_system='http://snomed.info/sct'
where condition_category.category_text='Problem List Item'
)

-- ============================== --

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

-- ============================== --

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

-- ============================== --

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

-- ============================== --

/*
  Exported: 2025-11-03T13:13:48Z
  Name: create-molecular-test-results
  ID: f2e1623f-bf0f-446f-8f75-1f23254db908
  Database: fhir_v2_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/f2e1623f-bf0f-446f-8f75-1f23254db908
  Description: 
*/
--drop table if exists molecular_test_results;
--select * from molecular_test_results;

create table molecular_test_results as(
select 
service_request.subject_reference as patient_id,
patient_access.mrn,
service_request.id as test_id,
diagnostic_report.id as result_diagnostic_report_id,
diagnostic_report.effective_date_time result_datetime,  
authored_on, 
sri.identifier_value as dgd_id,
service_request.code_text as lab_test_name, 
service_request.status as lab_test_status,
diagnostic_report_result.result_display test_component,
observation.value_string as test_result_narrative,
service_request.requester_display as lab_test_requester
from service_request 
left join service_request_category src on service_request.id=src.service_request_id
inner join patient_access on patient_access.id=service_request.subject_reference
left join service_request_identifier sri on service_request.id=sri.service_request_id
inner join diagnostic_report_based_on drbo on service_request.id=drbo.based_on_reference --only include service requests that have results returned for them. this filters out all the ancillary request types created by fhir and matches what would be seen in the EHR
left join diagnostic_report on drbo.diagnostic_report_id=diagnostic_report.id
left join diagnostic_report_result on diagnostic_report.id=diagnostic_report_result.diagnostic_report_id
left join fhir_v1_prd_db.observation on diagnostic_report_result.result_reference=observation.id
where 
service_request.status<>'revoked'
and (sri.identifier_system='https://open.epic.com/FHIR/20/order-accession-number/Beaker'
and sri.identifier_value like '%-GD-%')
or (sri.identifier_system='urn:oid:1.2.840.114350.1.13.20.2.7.3.798268.800' and sri.identifier_value like 'DGD%')
and service_request.code_text not in ('Normal specimen for Paired Tumor/Normal - Solid Tumor Panels','Solid Tumor Panel - Normal Paired') --exclude the test for the normal pair, as results are always attached to the tumor sample

--exclude result narratives that are just physician signatures, generic test info, or specimen preservation info
and diagnostic_report_result.result_display not in (
    'Component (1): GENOMICS SIGNATURES',
    'Component (1): TEST SPECIFIC INFORMATION',
    'Component (1): GDL SPECIMEN TREATMENT')
order by service_request.id
)

-- ============================== --

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

-- ============================== --

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

-- ============================== --

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

-- ============================== --

/*
  Exported: 2025-11-03T13:13:48Z
  Name: find-new-conditions
  ID: e9e71566-5162-40bd-880a-1179a22bdcad
  Database: fhir_chop_inc_2025_10_23_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/e9e71566-5162-40bd-880a-1179a22bdcad
  Description: 
*/
select '21st', count(distinct condition_id) conditions, count(distinct mrn) participants
from fhir_cbtn_inc_2025_10_21_prd_db.problem_list_diagnoses
where condition_id not in (select condition_id from fhir_prd_db.problem_list_diagnoses)

union

select '22nd', count(distinct condition_id), count(distinct mrn)
from fhir_cbtn_inc_2025_10_22_prd_db.problem_list_diagnoses
where condition_id not in (select condition_id from fhir_prd_db.problem_list_diagnoses
                            union select condition_id from fhir_cbtn_inc_2025_10_21_prd_db.problem_list_diagnoses)
                            
union

select '23rd', count(distinct condition_id), count(distinct mrn)
from fhir_chop_inc_2025_10_23_prd_db.problem_list_diagnoses
where condition_id not in (select condition_id from fhir_prd_db.problem_list_diagnoses
                            union select condition_id from fhir_cbtn_inc_2025_10_21_prd_db.problem_list_diagnoses
                            union select condition_id from fhir_cbtn_inc_2025_10_22_prd_db.problem_list_diagnoses)
order by 1

-- ============================== --

/*
  Exported: 2025-11-03T13:13:48Z
  Name: create-patient-access
  ID: 427d830b-98cb-491e-98c3-000bbc61af83
  Database: fhir_v2_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/427d830b-98cb-491e-98c3-000bbc61af83
  Description: create the patient access table with only patients consented to cbtn
*/
create table patient_access as(
select id, identifier_mrn as mrn, gender, race, ethnicity, given_name, family_name, birth_date, deceased_boolean, address_postal_code
from patient
where identifier_mrn in (
    select mrn
    from cbtn_enrolled_patients
    )
)

-- ============================== --

/*
  Exported: 2025-11-03T13:13:48Z
  Name: create-radiology-imaging-mri
  ID: 50fea73a-ca13-412f-a6e0-b9d571d46334
  Database: fhir_v2_prd_db
  Workgroup: primary
  ARN: arn:aws:athena:us-east-1:343218191717:namedquery/50fea73a-ca13-412f-a6e0-b9d571d46334
  Description: 
*/
--drop table if exists radiology_imaging_mri;
--select * from radiology_imaging_mri;

create table radiology_imaging_mri as (
select distinct 
service_request.subject_reference as patient_id,
patient_access.mrn,
service_request.id as imaging_procedure_id,
diagnostic_report.id as result_diagnostic_report_id,
diagnostic_report.effective_date_time result_datetime,  
authored_on, 
service_request_identifier.identifier_value as accession_number,
service_request.code_text as imaging_procedure, 
service_request.occurrence_date_time as imaging_datetime,
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
and lower(service_request.code_text) like 'mr%'
order by patient_id, service_request.authored_on desc)

-- ============================== --

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

-- ============================== --

