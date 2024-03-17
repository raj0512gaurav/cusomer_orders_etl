-- Set Context
use schema customer_orders.curated_zone;

-- Streams Creation
create or replace stream curated_customers_stm on table curated_customers;
create or replace stream curated_products_stm on table curated_products;
create or replace stream curated_orders_stm on table curated_orders;

-- Change context
use schema customer_orders.consumption_zone;

--Tasks Creation
create or replace task consumption_customers_tsk
    warehouse = compute_wh,
    schedule = '4 minutes'
when
    system$stream_has_data('customer_orders.curated_zone.curated_customers_stm')
as
    merge into customer_orders.consumption_zone.customers_dim customers_dim
    using customer_orders.curated_zone.curated_customers_stm curated_customers_stm on
    customers_dim.customer_id = curated_customers_stm.customer_id
    when matched
        and curated_customers_stm.METADATA$ACTION = 'INSERT'
        and curated_customers_stm.METADATA$ISUPDATE = 'TRUE'
    then update set
        customers_dim.customer_id = curated_customers_stm.customer_id,
        customers_dim.customer_email = curated_customers_stm.customer_email,
        customers_dim.customer_name = curated_customers_stm.customer_name,
        customers_dim.segment = curated_customers_stm.segment,
        customers_dim.country = curated_customers_stm.country,
        customers_dim.city = curated_customers_stm.city,
        customers_dim.state = curated_customers_stm.state,
        customers_dim.postal_code = curated_customers_stm.postal_code,
        customers_dim.region = curated_customers_stm.region
    when matched
        and curated_customers_stm.METADATA$ACTION = 'DELETE'
        and curated_customers_stm.METADATA$ISUPDATE = 'FALSE'
    then update set
        customers_dim.active_flag = 'N',
        customers_dim.updated_timestamp = current_timestamp()
    when not matched
        and curated_customers_stm.METADATA$ACTION = 'INSERT'
        and curated_customers_stm.METADATA$ISUPDATE = 'FALSE'
    then insert (
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
    values (
        curated_customers_stm.customer_id,
        curated_customers_stm.customer_email,
        curated_customers_stm.customer_name,  
        curated_customers_stm.segment,
        curated_customers_stm.country,
        curated_customers_stm.city,
        curated_customers_stm.state,
        curated_customers_stm.postal_code,
        curated_customers_stm.region
    );

create or replace task consumption_products_tsk
    warehouse = compute_wh,
    schedule = '5 minutes'
when
    system$stream_has_data('customer_orders.curated_zone.curated_products_stm')
as
    merge into customer_orders.consumption_zone.products_dim products_dim
    using customer_orders.curated_zone.curated_products_stm curated_products_stm on
    products_dim.product_id = curated_products_stm.product_id
    when matched
        and curated_products_stm.METADATA$ACTION = 'INSERT'
        and curated_products_stm.METADATA$ISUPDATE = 'TRUE'
    then update set
        products_dim.product_id = curated_products_stm.product_id,
        products_dim.product_name = curated_products_stm.product_name,
        products_dim.colors = curated_products_stm.colors,
        products_dim.category = curated_products_stm.category,
        products_dim.sub_category = curated_products_stm.sub_category,
        products_dim.date_added = curated_products_stm.date_added,
        products_dim.manufacturer = curated_products_stm.manufacturer
    when matched
        and curated_products_stm.METADATA$ACTION = 'DELETE'
        and curated_products_stm.METADATA$ISUPDATE = 'FALSE'
    then update set
        products_dim.active_flag = 'N',
        products_dim.updated_timestamp = current_timestamp()
    when not matched
        and curated_products_stm.METADATA$ACTION = 'INSERT'
        and curated_products_stm.METADATA$ISUPDATE = 'FALSE'
    then insert (
        product_id,
        product_name,
        colors,
        category,
        sub_category,
        date_added,
        manufacturer
    )
    values (
        curated_products_stm.product_id,
        curated_products_stm.product_name,
        curated_products_stm.colors,
        curated_products_stm.category,
        curated_products_stm.sub_category,
        curated_products_stm.date_added,
        curated_products_stm.manufacturer
    );

create or replace task orders_fact_tsk
    warehouse = compute_wh,
    schedule = '6 minutes'
when
    system$stream_has_data('customer_orders.curated_zone.curated_orders_stm')
as
    insert overwrite into customer_orders.consumption_zone.orders_fact (
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
            from customer_orders.curated_zone.curated_orders
            group by 1,2,3
        )
    order by order_year desc, month_number desc;

create or replace task customer_sales_fact_tsk
    warehouse = compute_wh,
    schedule = '6 minutes'
when
    system$stream_has_data('customer_orders.curated_zone.curated_orders_stm')
as
    insert overwrite into customer_orders.consumption_zone.customer_sales_fact (
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
    

create or replace task product_sales_fact_tsk
    warehouse = compute_wh,
    schedule = '6 minutes'
when
    system$stream_has_data('customer_orders.curated_zone.curated_orders_stm')
as
    insert overwrite into customer_orders.consumption_zone.product_sales_fact (
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

-- Start all tasks
alter task CONSUMPTION_CUSTOMERS_TSK resume;
alter task CONSUMPTION_PRODUCTS_TSK resume;
alter task CUSTOMER_SALES_FACT_TSK resume;
alter task ORDERS_FACT_TSK resume;
alter task PRODUCT_SALES_FACT_TSK resume;

show tasks;