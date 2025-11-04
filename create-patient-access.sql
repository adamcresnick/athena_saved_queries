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
