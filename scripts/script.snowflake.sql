DROP DATABASE IF EXISTS redfin_database_1;

CREATE DATABASE redfin_database_1;

CREATE SCHEMA redfin_schema;


CREATE OR REPLACE TABLE redfin_database_1.redfin_schema.redfin_table (
    period_begin DATE,
    period_end DATE,
    period_duration INT,
    region_type STRING,
    region_type_id INT,
    table_id INT,
    is_seasonally_adjusted STRING,
    city STRING,
    state STRING,
    state_code STRING,
    property_type STRING,
    property_type_id INT,
    median_sale_price FLOAT,
    median_list_price FLOAT,
    median_ppsf FLOAT,
    median_list_ppsf FLOAT,
    homes_sold FLOAT,
    inventory FLOAT,
    months_of_supply FLOAT,
    median_dom FLOAT,
    avg_sale_to_list FLOAT,
    sold_above_list FLOAT,
    parent_metro_region_metro_code STRING,
    last_updated DATETIME,
    period_begin_in_years STRING,
    period_end_in_years STRING,
    period_begin_in_months STRING,
    period_end_in_months STRING
);


CREATE SCHEMA file_format_schema;
CREATE OR REPLACE file format redfin_database_1.file_format_schema.format_csv
    type = 'CSV'
    field_delimiter = ','
    RECORD_DELIMITER = '\n'
    skip_header = 1

CREATE SCHEMA external_stage_schema;

CREATE OR REPLACE STAGE redfin_database_1.external_stage_schema.redfin_ext_stage_yml 
    url="s3://redfin-data-project-trandinhduy-2025/"
    credentials=()
    FILE_FORMAT = redfin_database_1.file_format_schema.format_csv;




