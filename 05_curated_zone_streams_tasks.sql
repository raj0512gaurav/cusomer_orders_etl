-- Set Context
use schema customer_orders.landing_zone;

-- Streams Creation
create or replace stream landing_customers_stm on table landing_customers
append_only = true;

create or replace stream landing_products_stm on table landing_products
append_only = true;

create or replace stream landing_orders_stm on table landing_orders
append_only = true;

-- Change context
use schema customer_orders.curated_zone;

-- Tasks Creation
create or replace task curated_orders_tsk
    warehouse = compute_wh
    schedule = '1 minute'
when
    system$stream_has_data('customer_orders.landing_zone.landing_orders_stm')
as
    merge into customer_orders.curated_zone.curated_orders curated_orders
    using customer_orders.landing_zone.landing_orders_stm landing_orders_stm on
    curated_orders.order_id = landing_orders_stm.order_id and
    curated_orders.product_id = landing_orders_stm.product_id
    when matched then
        update set
            curated_orders.order_id = landing_orders_stm.order_id,
            curated_orders.product_id = landing_orders_stm.product_id,
            curated_orders.sales = landing_orders_stm.sales,
            curated_orders.quantity = landing_orders_stm.quantity,
            curated_orders.discount = landing_orders_stm.discount,
            curated_orders.profit = landing_orders_stm.profit,
            curated_orders.customer_id = landing_orders_stm.customer_id,
            curated_orders.ship_mode = landing_orders_stm.ship_mode,
            curated_orders.order_status = landing_orders_stm.order_status,
            curated_orders.purchase_date = TO_TIMESTAMP(landing_orders_stm.purchase_date, 'MM/DD/YYYY HH24:MI'),
            curated_orders.delivered_date = TO_TIMESTAMP(landing_orders_stm.delivered_date, 'MM/DD/YYYY HH24:MI')
    when not matched then
        insert (
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
        values (
            landing_orders_stm.order_id,
            landing_orders_stm.product_id,
            landing_orders_stm.sales,
            landing_orders_stm.quantity,
            landing_orders_stm.discount,
            landing_orders_stm.profit,
            landing_orders_stm.customer_id,
            landing_orders_stm.ship_mode,
            landing_orders_stm.order_status,
            TO_TIMESTAMP(landing_orders_stm.purchase_date, 'MM/DD/YYYY HH24:MI'),
            TO_TIMESTAMP(landing_orders_stm.delivered_date, 'MM/DD/YYYY HH24:MI')
        );

create or replace task curated_customers_tsk
    warehouse = compute_wh
    schedule = '2 minute'
when
    system$stream_has_data('customer_orders.landing_zone.landing_customers_stm')
as
    merge into customer_orders.curated_zone.curated_customers curated_customers
    using customer_orders.landing_zone.landing_customers_stm landing_customers_stm on
    curated_customers.customer_id = landing_customers_stm.customer_id
    when matched then
        update set
            curated_customers.customer_id = landing_customers_stm.customer_id,
            curated_customers.customer_email = landing_customers_stm.customer_email,
            curated_customers.customer_name = landing_customers_stm.customer_name,
            curated_customers.segment = landing_customers_stm.segment,
            curated_customers.country = landing_customers_stm.country,
            curated_customers.city = landing_customers_stm.city,
            curated_customers.state = landing_customers_stm.state,
            curated_customers.postal_code = landing_customers_stm.postal_code,
            curated_customers.region = landing_customers_stm.region
    when not matched then
        insert (
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
            landing_customers_stm.customer_id,
            landing_customers_stm.customer_email,
            landing_customers_stm.customer_name,  
            landing_customers_stm.segment,
            landing_customers_stm.country,
            landing_customers_stm.city,
            landing_customers_stm.state,
            landing_customers_stm.postal_code,
            landing_customers_stm.region
        );

create or replace task curated_products_tsk
    warehouse = compute_wh
    schedule = '3 minute'
when
    system$stream_has_data('customer_orders.landing_zone.landing_products_stm')
as
    merge into customer_orders.curated_zone.curated_products curated_products
    using customer_orders.landing_zone.landing_products_stm landing_products_stm on
    curated_products.product_id = landing_products_stm.product_id
    when matched then
        update set
            curated_products.product_id = landing_products_stm.product_id,
            curated_products.product_name = landing_products_stm.product_name,
            curated_products.colors = landing_products_stm.colors,
            curated_products.category = landing_products_stm.category,
            curated_products.sub_category = landing_products_stm.sub_category,
            curated_products.date_added = TO_DATE(landing_products_stm.date_added, 'MM/DD/YYYY'),
            curated_products.manufacturer = landing_products_stm.manufacturer
    when not matched then
        insert (
            product_id,
            product_name,
            colors,
            category,
            sub_category,
            date_added,
            manufacturer
        )
        values (
            landing_products_stm.product_id,
            landing_products_stm.product_name,
            landing_products_stm.colors,
            landing_products_stm.category,
            landing_products_stm.sub_category,
            TO_DATE(landing_products_stm.date_added, 'MM/DD/YYYY'),
            landing_products_stm.manufacturer
        );

-- Start all tasks
alter task curated_orders_tsk suspend;
alter task curated_customers_tsk suspend;
alter task curated_products_tsk suspend;

show tasks;