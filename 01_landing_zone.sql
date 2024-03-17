-- Database and Schema setup
create database customer_orders;

create or replace schema landing_zone;
create or replace schema curated_zone;
create or replace schema consumption_zone;

--Customers, Orders and Products Transient Tables Creation
use schema landing_zone;

create or replace transient table landing_products (
    product_id varchar,
    product_name varchar,
    colors varchar,
    category varchar,
    sub_category varchar,
    date_added varchar,
    manufacturer varchar
) comment ='products table in landing schema';

create or replace transient table landing_orders (
    order_id varchar,
    product_id varchar,
    sales varchar,
    quantity varchar,
    discount varchar,
    profit varchar,
    customer_id varchar,
    ship_mode varchar,
    order_status varchar,
    purchase_date varchar,
    delivered_date varchar
) comment ='orders table in landing schema';

create or replace transient table landing_customers (
    customer_id varchar,
    customer_email varchar,
    customer_name varchar,
    segment varchar,
    country varchar,
    city varchar,
    state varchar,
    postal_code varchar,
    region varchar
) comment ='customers table in landing schema';

-- Create file format for csv files
create or replace file format my_csv_vi_webui
type = 'csv'
compression = 'auto'
field_delimiter = ','
record_delimiter = '\n'
skip_header = 1
field_optionally_enclosed_by = '"'
null_if = ('\\N');

show tables;