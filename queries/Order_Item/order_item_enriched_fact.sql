/*
Defines enriched.order_item_fact table as the central fact table at item level.
Stores measures (quantity, price, subtotal) and links to all relevant dimensions (customer, address, restaurant, location, menu, delivery agent, date).
Populates data via MERGE from curated streams joined with enriched dimensions.
Adds foreign key constraints to enforce referential integrity.
*/
USE ROLE data_engineer;
USE SCHEMA foodpanda_db.enriched;

CREATE OR REPLACE TABLE enriched.order_item_fact (
    order_item_fact_sk NUMBER AUTOINCREMENT PRIMARY KEY COMMENT 'Surrogate Key (EDW)',
    order_item_id NUMBER NOT NULL COMMENT 'Order Item natural key (Source System)',
    order_id NUMBER NOT NULL COMMENT 'Order natural key (Source System)',
    customer_dim_key NUMBER COMMENT 'Customer dimension surrogate key',
    customer_address_dim_key NUMBER COMMENT 'Customer address dimension surrogate key',
    restaurant_dim_key NUMBER COMMENT 'Restaurant dimension surrogate key',
    restaurant_location_dim_key NUMBER COMMENT 'Restaurant location dimension surrogate key',
    menu_dim_key NUMBER COMMENT 'Menu dimension surrogate key',
    delivery_agent_dim_key NUMBER COMMENT 'Delivery agent dimension surrogate key',
    order_date_dim_key NUMBER COMMENT 'Date dimension surrogate key',
    quantity NUMBER COMMENT 'Item quantity',
    price NUMBER(10,2) COMMENT 'Unit price',
    subtotal NUMBER(10,2) COMMENT 'Line subtotal',
    delivery_status STRING COMMENT 'Delivery status',
    estimated_time STRING COMMENT 'Estimated delivery time'
)
COMMENT = 'Order item fact table storing item-level measures and foreign keys to dimensions.';

MERGE INTO enriched.order_item_fact AS target
USING (
    SELECT 
        TRY_TO_NUMBER(oi.order_item_id) AS order_item_id,
        TRY_TO_NUMBER(oi.order_id_fk) AS order_id,
        c.customer_hk AS customer_dim_key,
        ca.customer_address_hk AS customer_address_dim_key,
        r.restaurant_hk AS restaurant_dim_key, 
        rl.restaurant_location_hk AS restaurant_location_dim_key,
        m.menu_dim_hk AS menu_dim_key,
        da.delivery_agent_hk AS delivery_agent_dim_key,
        dd.date_dim_hk AS order_date_dim_key,
        TRY_TO_NUMBER(oi.quantity) AS quantity,
        TRY_TO_DECIMAL(oi.price,10,2) AS price,
        TRY_TO_DECIMAL(oi.subtotal,10,2) AS subtotal,
        d.delivery_status AS delivery_status,
        d.estimated_time AS estimated_time
    FROM curated.order_item_stm oi
    JOIN curated.orders_stm o ON oi.order_id_fk = o.order_id
    JOIN curated.delivery_stm d ON o.order_id = d.order_id_fk
    JOIN enriched.customer_dim c ON o.customer_id_fk = c.customer_id
    JOIN enriched.customer_address_dim ca ON c.customer_id = ca.customer_id_fk
    JOIN enriched.restaurant_dim r ON o.restaurant_id_fk = r.restaurant_id
    JOIN enriched.menu_dim m ON oi.menu_id_fk = m.menu_id
    JOIN enriched.delivery_agent_dim da ON d.delivery_agent_id_fk = da.delivery_agent_id
    JOIN enriched.restaurant_location_dim rl ON r.location_id_fk = rl.location_id
    JOIN enriched.date_dim dd ON dd.calendar_date = DATE(o.order_date)
) AS source_stm
ON target.order_item_id = source_stm.order_item_id
   AND target.order_id = source_stm.order_id
WHEN MATCHED THEN
    UPDATE SET
        target.customer_dim_key = source_stm.customer_dim_key,
        target.customer_address_dim_key = source_stm.customer_address_dim_key,
        target.restaurant_dim_key = source_stm.restaurant_dim_key,
        target.restaurant_location_dim_key = source_stm.restaurant_location_dim_key,
        target.menu_dim_key = source_stm.menu_dim_key,
        target.delivery_agent_dim_key = source_stm.delivery_agent_dim_key,
        target.order_date_dim_key = source_stm.order_date_dim_key,
        target.quantity = source_stm.quantity,
        target.price = source_stm.price,
        target.subtotal = source_stm.subtotal,
        target.delivery_status = source_stm.delivery_status,
        target.estimated_time = source_stm.estimated_time
WHEN NOT MATCHED THEN
    INSERT (
        order_item_id,
        order_id,
        customer_dim_key,
        customer_address_dim_key,
        restaurant_dim_key,
        restaurant_location_dim_key,
        menu_dim_key,
        delivery_agent_dim_key,
        order_date_dim_key,
        quantity,
        price,
        subtotal,
        delivery_status,
        estimated_time
    )
    VALUES (
        source_stm.order_item_id,
        source_stm.order_id,
        source_stm.customer_dim_key,
        source_stm.customer_address_dim_key,
        source_stm.restaurant_dim_key,
        source_stm.restaurant_location_dim_key,
        source_stm.menu_dim_key,
        source_stm.delivery_agent_dim_key,
        source_stm.order_date_dim_key,
        source_stm.quantity,
        source_stm.price,
        source_stm.subtotal,
        source_stm.delivery_status,
        source_stm.estimated_time
    );

-- Foreign key constraints
ALTER TABLE enriched.order_item_fact
    ADD CONSTRAINT fk_order_item_fact_customer_dim
    FOREIGN KEY (customer_dim_key) REFERENCES enriched.customer_dim (customer_hk);

ALTER TABLE enriched.order_item_fact
    ADD CONSTRAINT fk_order_item_fact_customer_address_dim
    FOREIGN KEY (customer_address_dim_key) REFERENCES enriched.customer_address_dim (customer_address_hk);

ALTER TABLE enriched.order_item_fact
    ADD CONSTRAINT fk_order_item_fact_restaurant_dim
    FOREIGN KEY (restaurant_dim_key) REFERENCES enriched.restaurant_dim (restaurant_hk);

ALTER TABLE enriched.order_item_fact
    ADD CONSTRAINT fk_order_item_fact_restaurant_location_dim
    FOREIGN KEY (restaurant_location_dim_key) REFERENCES enriched.restaurant_location_dim (restaurant_location_hk);

ALTER TABLE enriched.order_item_fact
    ADD CONSTRAINT fk_order_item_fact_menu_dim
    FOREIGN KEY (menu_dim_key) REFERENCES enriched.menu_dim (menu_dim_hk);

ALTER TABLE enriched.order_item_fact
    ADD CONSTRAINT fk_order_item_fact_delivery_agent_dim
    FOREIGN KEY (delivery_agent_dim_key) REFERENCES enriched.delivery_agent_dim (delivery_agent_hk);

ALTER TABLE enriched.order_item_fact
    ADD CONSTRAINT fk_order_item_fact_delivery_date_dim
    FOREIGN KEY (order_date_dim_key) REFERENCES enriched.date_dim (date_dim_hk);
