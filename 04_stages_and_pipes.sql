-- Set Context
use database customer_orders;
use schema landing_zone;

-- Create storage integration object
create or replace STORAGE INTEGRATION s3_int
  type = external_stage
  storage_provider = s3
  storage_aws_role_arn = 'arn:aws:iam::730335616989:role/snowflake_access_role'
  enabled = true
  storage_allowed_locations = ('s3://snowflake-raj/');

desc storage integration s3_int;

-- Create External Stages for S3 bucket
CREATE STAGE customers_s3
  URL='s3://snowflake-raj/customers_orders_etl/customers'
  storage_integration = s3_int;

CREATE STAGE products_s3
  URL='s3://snowflake-raj/customers_orders_etl/products'
  storage_integration = s3_int;

CREATE STAGE orders_s3
  URL='s3://snowflake-raj/customers_orders_etl/orders'
  storage_integration = s3_int;

-- Pipe objects to push files via copy command
create or replace pipe orders_pipe
    auto_ingest = true
as 
    copy into landing_orders from @orders_s3
    file_format = MY_CSV_VI_WEBUI
    pattern='.*orders.*[.]csv'
    ON_ERROR = 'CONTINUE';

create or replace pipe products_pipe
    auto_ingest = true
as 
    copy into landing_products from @products_s3
    file_format = MY_CSV_VI_WEBUI
    pattern='.*products.*[.]csv'
    ON_ERROR = 'CONTINUE';

create or replace pipe customers_pipe
    auto_ingest = true
as 
    copy into landing_customers from @customers_s3
    file_format = MY_CSV_VI_WEBUI
    pattern='.*customers.*[.]csv'
    ON_ERROR = 'CONTINUE';