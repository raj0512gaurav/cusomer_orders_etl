-- Set Context
use database customer_orders;
use schema curated_zone;

--Transient Tables Creation
create or replace transient table curated_products (
    product_pk number autoincrement primary key,
    product_id varchar(20) not null,
    product_name varchar not null,
    colors varchar(15),
    category varchar(20),
    sub_category varchar(20),
    date_added date,
    manufacturer varchar
) comment ='products table in curated schema';

create or replace transient table curated_orders (
    order_pk number autoincrement primary key,
    order_id varchar(20) not null,
    product_id varchar(20) not null,
    sales number(7,2),
    quantity number,
    discount number(7,2),
    profit number(7,2),
    customer_id varchar(20),
    ship_mode varchar(20),
    order_status varchar(15),
    purchase_date timestamp_ntz,
    delivered_date timestamp_ntz
) comment ='orders table in curated schema';

create or replace transient table curated_customers (
    customer_pk number autoincrement primary key,
    customer_id varchar(20) not null,
    customer_email varchar not null,
    customer_name varchar not null,
    segment varchar(20),
    country varchar,
    city varchar,
    state varchar,
    postal_code number,
    region varchar(10)
) comment ='customers table in curated schema';

--Insert Data
insert into curated_customers (
    customer_id,
    customer_email,
    customer_name,  
    segment,
    country,
    city,
    state,
    postal_code,
    region
)
select
    customer_id,
    customer_email,
    customer_name,  
    segment,
    country,
    city,
    state,
    postal_code,
    region
from landing_zone.landing_customers;

insert into curated_orders (
    order_id,
    product_id,
    sales,
    quantity,
    discount,
    profit,
    customer_id,
    ship_mode,
    order_status,
    purchase_date,
    delivered_date
)
select
    order_id,
    product_id,
    sales,
    quantity,
    discount,
    profit,
    customer_id,
    ship_mode,
    order_status,
    TO_TIMESTAMP(purchase_date, 'MM/DD/YYYY HH24:MI'),
    TO_TIMESTAMP(delivered_date, 'MM/DD/YYYY HH24:MI')
from landing_zone.landing_orders;

insert into curated_products (
    product_id,
    product_name,
    colors,
    category,
    sub_category,
    date_added,
    manufacturer
)
select
    product_id,
    product_name,
    colors,
    category,
    sub_category,
    TO_DATE(date_added, 'MM/DD/YYYY'),
    manufacturer
from landing_zone.landing_products;

show tables;