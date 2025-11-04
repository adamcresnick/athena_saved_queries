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
