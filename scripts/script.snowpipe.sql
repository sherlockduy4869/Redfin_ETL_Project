CREATE OR REPLACE SCHEMA redfin_database_1.snowpipe_schema;
CREATE OR REPLACE PIPE redfin_database_1.snowpipe_schema.redfin_snowpipe
auto_ingest = TRUE
AS 
COPY INTO redfin_database_1.redfin_schema.redfin_table
FROM @redfin_database_1.external_stage_schema.redfin_ext_stage_yml;

DESC PIPE redfin_database_1.snowpipe_schema.redfin_snowpipe;