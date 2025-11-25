/*
This script initializes the Foodpanda Bangladesh Snowflake environment by creating the warehouse,
schemas (raw, curated, enriched, metadata), file formats, staging areas, and data masking policies
to support the end-to-end data pipeline.
*/

-- Use a dedicated role for data engineering
use role data_engineer;

-- Create a dedicated warehouse for Foodpanda pipeline workloads
create warehouse if not exists foodpanda_wh
    comment = 'Warehouse for Foodpanda data pipeline'
    warehouse_size = 'x-small'
    auto_resume = true
    auto_suspend = 60
    enable_query_acceleration = false
    warehouse_type = 'standard'
    min_cluster_count = 1
    max_cluster_count = 1
    scaling_policy = 'standard'
    initially_suspended = true;

create database if not exists foodpanda_db;
use database foodpanda_db;

-- Create database schemas following medallion architecture
create schema if not exists raw;       -- staging layer
create schema if not exists curated;   -- cleansed layer
create schema if not exists enriched;  -- analytics-ready layer
create schema if not exists metadata;  -- governance and policies

-- Switch to raw schema for staging operations
use schema raw;

-- Define file format for CSV ingestion
create file_format if not exists raw.csv_format
        type = 'csv'
        compression = 'auto'
        field_delimiter = ','
        record_delimiter = '\n'
        skip_header = 1
        field_optionally_enclosed_by = '\042'
        null_if = ('\\N');

-- Create internal stage for raw CSV ingestion
create stage raw.csv_stage
    directory = (enable = true)
    comment = 'Internal stage for raw CSV ingestion';

-- Create governance tags and masking policies in metadata schema
create or replace tag 
    metadata.data_sensitivity_tag 
    allowed_values 'PII','PRICE','SENSITIVE','EMAIL'
    comment = 'Tag for sensitive data classification';

create or replace masking policy 
    metadata.mask_pii as (pii_text string)
    returns string -> to_varchar('** PII **');

create or replace masking policy 
    metadata.mask_email as (email_text string)
    returns string -> to_varchar('** EMAIL **');

create or replace masking policy 
    metadata.mask_phone as (phone string)
    returns string -> to_varchar('** PHONE **');

-- List files staged for ingestion
list @raw.csv_stage/initial;

-- Preview staged CSV data using positional $ notation
select 
    t.$1::text as locationid,
    t.$2::text as city,
    t.$3::text as state,
    t.$4::text as zipcode,
    t.$5::text as activeflag,
    t.$6::text as createddate,
    t.$7::text as modifieddate
from @raw.csv_stage/initial/Location.csv (file_format => 'raw.csv_format') t;
