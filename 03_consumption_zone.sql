-- Set Context
use database customer_orders;
use schema consumption_zone;

--Tables Creation
create or replace table products_dim (
    product_dim_key number autoincrement primary key,
    product_id varchar(20) not null,
    product_name varchar not null,
    colors varchar(15),
    category varchar(20),
    sub_category varchar(20),
    date_added date,
    manufacturer varchar,
    added_timestamp timestamp default current_timestamp() ,
    updated_timestamp timestamp default current_timestamp() ,
    active_flag varchar(1) default 'Y'
) comment ='products table in consumption schema';

create or replace table customers_dim (
    customer_dim_key number autoincrement primary key,
    customer_id varchar(20) not null,
    customer_email varchar not null,
    customer_name varchar not null,
    segment varchar(20),
    country varchar,
    city varchar,
    state varchar,
    postal_code number,
    region varchar(10),
    added_timestamp timestamp default current_timestamp() ,
    updated_timestamp timestamp default current_timestamp() ,
    active_flag varchar(1) default 'Y'
) comment ='customers table in consumption schema';

create or replace table orders_fact (
    order_fact_key number autoincrement primary key,
    order_year number,
    order_month varchar(10),
    order_count number,
    total_sales number(20,2),
    total_sales_quantity number,
    total_discount_amt number(10,2),
    net_profit number(20,2)
) comment ='monthly orders fact table in consumption schema';

create or replace table customer_sales_fact (
    customer_sales_fact_key number autoincrement primary key,
    customer_dim_key number,
    order_count number,
    amount_spent number(7,2),
    avg_puchase_freqeuncy number(7,2)
) comment ='customer wise sales fact table in consumption schema';

create or replace table product_sales_fact (
    product_sales_fact_key number autoincrement primary key,
    product_dim_key number,
    total_sales number(7,2),
    avg_quantity_ordered number(7,2)
) comment ='product wise sales fact table in consumption schema';

--Insert Data
insert into products_dim (
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
    date_added,
    manufacturer
from curated_zone.curated_products;

insert into customers_dim (
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
from curated_zone.curated_customers;

insert into orders_fact (
    order_year,
    order_month,
    order_count,
    total_sales,
    total_sales_quantity,
    total_discount_amt,
    net_profit
)
select
    order_year,
    month_name,
    order_count,
    total_sales,
    total_sales_quantity,
    total_discount_amt,
    net_profit
    from (
        select
            YEAR(purchase_date) as order_year,
            MONTH(purchase_date) as month_number,
            MONTHNAME(purchase_date) month_name,
            count(*) as order_count,
            sum(sales) as total_sales,
            sum(quantity) as total_sales_quantity,
            sum(discount) as total_discount_amt,
            sum(profit) as net_profit
        from curated_zone.curated_orders
        group by 1,2,3
    )
order by order_year desc, month_number desc;

insert into customer_sales_fact (
    customer_dim_key,
    order_count,
    amount_spent,
    avg_puchase_freqeuncy
)
with purchase_gaps as (
    select
        customer_id,
        sales,
        DATEDIFF('day', LAG(purchase_date) OVER (PARTITION BY customer_id ORDER BY purchase_date), purchase_date) AS purchase_gap
    from curated_zone.curated_orders
)
select
    cd.customer_dim_key,
    count(*),
    sum(pgs.sales),
    avg(pgs.purchase_gap)
from purchase_gaps pgs
join customers_dim cd on pgs.customer_id = cd.customer_id
group by 1;

insert into product_sales_fact (
    product_dim_key,
    total_sales,
    avg_quantity_ordered
)
select
    pd.product_dim_key,
    sum(co.sales),
    avg(co.quantity)
from products_dim pd
join curated_zone.curated_orders co on co.product_id = pd.product_id
group by 1;

show tables;