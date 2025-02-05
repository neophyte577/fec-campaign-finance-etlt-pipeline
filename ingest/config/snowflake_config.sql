USE ROLE accountadmin;

CREATE OR REPLACE WAREHOUSE fec_wh 
    WITH 
        WAREHOUSE_SIZE ='x-small'
        AUTO_SUSPEND = 60 
        AUTO_RESUME = TRUE;

CREATE OR REPLACE DATABASE fec_db;
CREATE OR REPLACE ROLE fec_role;

GRANT usage ON WAREHOUSE fec_wh TO ROLE fec_role;
GRANT ROLE fec_role TO USER neophyte577;
GRANT ALL ON DATABASE fec_db TO ROLE fec_role;

USE ROLE fec_role;

CREATE OR REPLACE SCHEMA fec_db.raw;
CREATE OR REPLACE SCHEMA fec_db.transformed;

SHOW GRANTS ON WAREHOUSE fec_wh;

USE ROLE accountadmin;

CREATE OR REPLACE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<iam_role>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://fec-data-staging-bucket/');

DESCRIBE INTEGRATION s3_integration;

GRANT CREATE STAGE ON SCHEMA fec_db.raw TO ROLE fec_role;

GRANT USAGE ON INTEGRATION s3_integration TO ROLE fec_role;

USE ROLE fec_role;

USE SCHEMA fec_db.raw;

CREATE OR REPLACE STAGE s3_stage
    STORAGE_INTEGRATION = s3_integration
    URL = 's3://fec-data-staging-bucket'
    FILE_FORMAT = (TYPE = 'CSV');

LIST @s3_stage;

SELECT * FROM TABLE(RESULT_SCAN(<query_id>));

CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = CSV
  SKIP_HEADER = 1
  RECORD_DELIMITER = '\n'
  FIELD_DELIMITER = ','
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '0x22';

-- USE ROLE accountadmin;

-- DROP WAREHOUSE IF EXISTS fec_wh;
-- DROP DATABASE IF EXISTS fec_db;
-- DROP ROLE IF EXISTS fec_role;

