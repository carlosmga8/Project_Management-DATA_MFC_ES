--Update WH KPIs for MP tool

WITH
ranges as (select date('2024-12-30') as rango_fecha
),

orders as (         --query for consult orders
    SELECT

        msa.country_code                                                                as country_code,
            case 
                when msa.city_code = 'BCN' then 'BCN' 
                when msa.city_code = 'MAD' then 'MAD' 
                else 'EXP' end                                                          as area_code,
        msa.city_code                                                                   as city_code,
        extract(year from o.order_activated_local_at)                                   as year,
        extract(month from o.order_activated_local_at)                                  as month,
        extract(week from o.order_activated_local_at)                                   as week,
        DATE_FORMAT(o.order_activated_local_at , '%Y-%m-%d')                            as "date",          
        case when msa.warehouse_name = 'VALF2 - Maestro Guerrero'  then 'VALF2 - Centelles' else msa.warehouse_name end    as warehouse_name,
        count(distinct case
                when o.order_parent_relationship_type is null then o.order_id
                else null end)                                                          as placed_orders,
        count(distinct case
                when o.order_final_status = 'DeliveredStatus'
                    and o.order_parent_relationship_type is null then o.order_id
                else null end)                                                          as delivered_orders,
        count(distinct case
                when o.order_final_status = 'CanceledStatus'
                    and o.order_parent_id is null
                    and not ((date_diff('second', o.order_created_local_at,
                    o.order_terminated_local_at) / 60.0 <= 5 -- cancel_time_before_5min_of_creation
                        or (date_diff('second', o.order_activated_local_at,
                    o.order_terminated_local_at) / 60.0 <= 5 -- cancel_time_before_5min_of_activation
                    and o.order_scheduled_local_at is not null) -- order_is_scheduled
                        or (o.order_scheduled_local_at is not null
                    and o.order_activated_local_at is null)) -- order_is_scheduled_but_not_activated
                    and (o.order_cancel_reason is null
                        or o.order_cancel_reason in ('SELF_CANCELLATION','UNKNOWN','OTHER',
                    'DELIVERY_TAKING_TOO_LONG','COURIER_NOT_ASSIGNED',
                    'CUSTOMER_DOESNT_WANT_PRODUCTS'))) -- order_cancel_reason is null or in OPS reasons
                then o.order_id else null end)                                          as n_orders_total_hard_cancellations,

        (1.0000 * (count(distinct case
                when o.order_final_status = 'CanceledStatus'
                    and o.order_parent_id is null
                    and not ((date_diff('second', o.order_created_local_at,
                    o.order_terminated_local_at) / 60.0 <= 5 -- cancel_time_before_5min_of_creation
                        or (date_diff('second', o.order_activated_local_at,
                    o.order_terminated_local_at) / 60.0 <= 5 -- cancel_time_before_5min_of_activation
                    and o.order_scheduled_local_at is not null) -- order_is_scheduled
                        or (o.order_scheduled_local_at is not null
                    and o.order_activated_local_at is null)) -- order_is_scheduled_but_not_activated
                    and (o.order_cancel_reason is null
                        or o.order_cancel_reason in ('SELF_CANCELLATION','UNKNOWN','OTHER',
                    'DELIVERY_TAKING_TOO_LONG','COURIER_NOT_ASSIGNED',
                    'CUSTOMER_DOESNT_WANT_PRODUCTS'))) -- order_cancel_reason is null or in OPS reasons
                then o.order_id else null end)) / 
            (count(distinct case
                when o.order_parent_relationship_type is null then o.order_id
                else null end)))                                                         as "%_HC",

        count(distinct case
                when o.order_final_status = 'DeliveredStatus'
                    and o.order_parent_relationship_type is null
                    and dta.delivery_time_in_seconds >= 60 * 60 then o.order_id
                else null end)                                                          as n_orders_DT_more_60,

        count(distinct case
                when o.order_final_status = 'DeliveredStatus'
                    and o.order_parent_relationship_type is not null then o.order_id
                else null end)                                                          as remakes

    FROM
      delta.central_order_descriptors_odp.order_descriptors_v2 as o
      LEFT JOIN delta.mfc_sales_odp.mfc_store_addresses_history as msa on o.store_address_id = msa.store_address_id
      and order_activated_local_at BETWEEN msa.valid_from AND msa.valid_to
      LEFT JOIN delta.courier_order_flow_odp.delivery_times_order_level_attributes as dta on dta.order_id = o.order_id

    WHERE
        o.p_creation_date >= (select rango_fecha from ranges)
        and o.order_activated_local_at >= (select rango_fecha from ranges)
        and msa.warehouse_name is not null
        and msa.country_code = 'ES'
        and o.order_handling_strategy = 'GEN2'
    GROUP BY
      1,2,3,4,5,6,7,8
    ORDER BY
      1,2,3,4,5,6,7,8
),

gmv as (            --query for consult GMV
    SELECT

        ps.country_code                                                                 as country_code,
        case 
            when ps.city_code = 'BCN' then 'BCN' 
            when ps.city_code = 'MAD' then 'MAD' 
            else 'EXP' end                                                              as area_code,
        ps.city_code                                                                    as city_code,
        year(ps.order_activation_local_datetime)                                        as year,
        month(ps.order_activation_local_datetime)                                       as month,
        week(ps.order_activation_local_datetime)                                        as week, 
        DATE_FORMAT(ps.order_activation_local_datetime , '%Y-%m-%d')                    as "date",   
        case when ps.warehouse_name = 'VALF2 - Maestro Guerrero'  then 'VALF2 - Centelles' else ps.warehouse_name end    as warehouse_name,        
        COUNT(DISTINCT ps.order_id)                                                     as total_orders,
        round(COALESCE(SUM(ps.product_total_revenue_net_eur), 0),2)                     as revenue_net_eur

    FROM 
        delta.mfc_sales_odp.products_sold_v3  AS ps
    
    WHERE (ps.country_code) = 'ES' 
        AND (ps.order_final_status) = 'DeliveredStatus' 
        AND ps.p_order_activation_local_date >= (select rango_fecha from ranges)
    
    GROUP BY
        1,2,3,4,5,6,7,8
    ORDER BY
        1,2,3,4,5,6,7,8
),

Sh as (             --query for consult Shrinkage
    SELECT

        qc.country_code                                                                 as country_code,
        case 
            when qc.city_code = 'BCN' then 'BCN' 
            when qc.city_code = 'MAD' then 'MAD' 
            else 'EXP' end                                                              as area_code,
        qc.city_code                                                                    as city_code,
        year (qc.p_inventory_level_local_date)                                          as year,
        month (qc.p_inventory_level_local_date )                                        as month, 
        week (qc.p_inventory_level_local_date )                                         as week, 
        DATE_FORMAT(qc.p_inventory_level_local_date , '%Y-%m-%d')                       as "date",
        case when qc.warehouse_name = 'VALF2 - Maestro Guerrero'  then 'VALF2 - Centelles' else qc.warehouse_name end    as warehouse_name,
        ROUND(COALESCE(SUM(qc.total_shrinkage_eur ), 0),2)                              as total_shrinkage_eur
    
    FROM 
        delta.mfc__catman_kpis_product_mfc_hour__odp.catman_kpis_product_mfc_hour  AS qc
    WHERE 
        (qc.country_code ) = 'ES' 
        AND qc.p_inventory_level_local_date >= (select rango_fecha from ranges)
    
    GROUP BY
        1,2,3,4,5,6,7,8
    ORDER BY
        1,2,3,4,5,6,7,8
),

ia as (             --query for consult Inventary adjustament
    SELECT
 
        il.country_code                                                                 as country_code,
        case 
            when il.city_code = 'BCN' then 'BCN' 
            when il.city_code = 'MAD' then 'MAD' 
            else 'EXP' end                                                              as area_code,
        il.city_code                                                                    as city_code,
        year (il.stock_movement_local_datetime)                                         as year,
        month (il.stock_movement_local_datetime)                                        as month, 
        week (il.stock_movement_local_datetime)                                         as week, 
        DATE_FORMAT(il.stock_movement_local_datetime, '%Y-%m-%d')                       as "date",
        case when il.warehouse_name = 'VALF2 - Maestro Guerrero'  then 'VALF2 - Centelles' else il.warehouse_name end    as warehouse_name,
        ROUND(COALESCE(SUM(CASE 
            WHEN il.is_inventory_regularization 
            THEN il.product_total_cost_eur  *  il.quantity_sign   
            ELSE NULL END), 0),2)                                                       as total_cost_of_inventory_regularization_eur

    FROM 
        delta.mfc_inventory_odp.inventory_loss_v3  AS il
    WHERE 
        (il.country_code) = 'ES' 
        AND il.stock_movement_local_datetime >= (select rango_fecha from ranges)
        AND (month(il.stock_movement_local_datetime )) IS NOT NULL
    
    GROUP BY
        1,2,3,4,5,6,7,8
    ORDER BY
        1,2,3,4,5,6,7,8
),

fr as (             --query for consult food rescue packs Orders
    SELECT

        ck.country_code                                                                 as country_code,
            case 
                when ck.city_code = 'BCN' then 'BCN' 
                when ck.city_code = 'MAD' then 'MAD' 
                else 'EXP' end                                                          as area_code,
        ck.city_code                                                                    as city_code,
        year(ck.p_inventory_level_local_date)                                           as year,
        month(ck.p_inventory_level_local_date)                                          as month,
        week(ck.p_inventory_level_local_date)                                           as week,
        DATE_FORMAT(ck.p_inventory_level_local_date, '%Y-%m-%d')                        as "date",
        case when ck.warehouse_name = 'VALF2 - Maestro Guerrero'  then 'VALF2 - Centelles' else ck.warehouse_name end    as warehouse_name,
        COALESCE(SUM(ck.product_quantity_sold ), 0)                                     as total_product_quantity_sold
    FROM 
        delta.mfc__catman_kpis_product_mfc_hour__odp.catman_kpis_product_mfc_hour  AS ck
    WHERE 
        (ck.product_sku ) = '366849' 
        AND ck.p_inventory_level_local_date >= (select rango_fecha from ranges)
    
    GROUP BY
        1,2,3,4,5,6,7,8
    ORDER BY
        1,2,3,4,5,6,7,8
),

br as (             --query for consult bad rating category ultra fresh Orders
    SELECT 

        ps.country_code                                                                 as country_code,
        case 
            when ps.city_code = 'BCN' then 'BCN' 
            when ps.city_code = 'MAD' then 'MAD' 
            else 'EXP' end                                                              as area_code,
        ps.city_code                                                                    as city_code,
        year(p_order_activation_local_date)                                             as year,
        month(p_order_activation_local_date)                                            as month,
        week(p_order_activation_local_date)                                             as week,
        DATE_FORMAT(p_order_activation_local_date, '%Y-%m-%d')                          as "date",
        case when ps.warehouse_name = 'VALF2 - Maestro Guerrero'  then 'VALF2 - Centelles' else ps.warehouse_name end    as warehouse_name,

        COUNT(DISTINCT 
            CASE WHEN (off.feedback_selected_option LIKE 'POOR_QUALITY') 
                AND (ps.order_final_status = 'DeliveredStatus') 
                AND ps.product_category_level_one IN ('Meat / Seafood','Produce','Ready To Consume') 
                THEN ps.order_id  ELSE NULL END)                                        as num_order_br_uf,  
        COUNT(DISTINCT 
            CASE WHEN ps.product_category_level_one IN ('Meat / Seafood','Produce','Ready To Consume') 
                AND (ps.order_final_status = 'DeliveredStatus') 
                THEN ps.order_id ELSE NULL END)                                         as ultra_fresh_delivered,
       
        CASE 
            WHEN COUNT(DISTINCT CASE 
                WHEN ps.product_category_level_one IN ('Meat / Seafood','Produce','Ready To Consume') 
                    AND (ps.order_final_status = 'DeliveredStatus') 
                    THEN ps.order_id ELSE NULL END) = 0 THEN 0 
                ELSE CAST(COUNT(DISTINCT CASE 
                    WHEN (off.feedback_selected_option LIKE 'POOR_QUALITY') 
                        AND (ps.order_final_status = 'DeliveredStatus') 
                        AND ps.product_category_level_one IN ('Meat / Seafood','Produce','Ready To Consume') 
                    THEN ps.order_id ELSE NULL END) AS DECIMAL(10, 4)) / 
                COUNT(DISTINCT CASE 
                    WHEN ps.product_category_level_one IN ('Meat / Seafood','Produce','Ready To Consume') 
                        AND (ps.order_final_status = 'DeliveredStatus') 
                    THEN ps.order_id ELSE NULL END)  END                                as percentage_br_uf

    FROM 
        delta.mfc_sales_odp.products_sold_v3 ps
    LEFT JOIN 
        delta.customer_bought_products_odp.bought_products_v2 AS bp
            ON ps.product_sku = bp.product_external_id
            AND ps.order_id = bp.order_id
    LEFT JOIN 
        delta.contact_contact_intent_odp.fct_contact_intent AS off
            ON bp.bought_product_id = off.bought_product_id
            AND off.p_created_date >= (select rango_fecha from ranges)
    WHERE 1=1
        AND ps.p_order_activation_local_date >= (select rango_fecha from ranges)
        AND bp.p_creation_date >= (select rango_fecha from ranges)
        AND ps.country_code = 'ES'
    GROUP BY 1,2,3,4,5,6,7,8
    order by 1,2,3,4,5,6,7,8
),

pna as (            --query for consult PNA Orders 
    SELECT

        ps.country_code                                                                 as country_code,
        case 
            when ps.city_code = 'BCN' then 'BCN' 
            when ps.city_code = 'MAD' then 'MAD' 
            else 'EXP' end                                                              as area_code,
        ps.city_code                                                                    as city_code,
        year (ps.order_activation_local_datetime)                                       as year,
        month (ps.order_activation_local_datetime)                                      as month,
        week (ps.order_activation_local_datetime)                                       as week,
        DATE_FORMAT(ps.order_activation_local_datetime, '%Y-%m-%d')                     as "date",
        case when ps.warehouse_name = 'VALF2 - Maestro Guerrero' 
        then 'VALF2 - Centelles' else ps.warehouse_name end                             as warehouse_name, 
        (1.0000 * ( COUNT(DISTINCT CASE 
            WHEN order_pna.order_is_pna THEN order_pna.order_id END )) 
            / nullif(( COUNT(DISTINCT order_pna.order_id ) ), 0))                       as percentage_of_orders_with_pna,
        COUNT(DISTINCT CASE 
            WHEN order_pna.order_is_pna THEN order_pna.order_id END)                    as n_pna_orders,
        nullif(( COUNT(DISTINCT order_pna.order_id ) ), 0)                              as total_order_pna    

    FROM 
        delta.mfc_sales_odp.products_sold_v3  AS ps  

    LEFT JOIN delta.logistics__product_not_available__odp.order_product_not_available  AS order_pna 
        ON order_pna.order_id = ps.order_id

    WHERE 
        (ps.warehouse_name IS NOT NULL OR ps.warehouse_business_id IS NOT NULL ) 
        AND (ps.country_code ) = 'ES'
        AND ps.order_activation_local_datetime >= (select rango_fecha from ranges)
    
    GROUP BY
        1,2,3,4,5,6,7,8
    ORDER BY
        1,2,3,4,5,6,7,8
),

pup as (            --query for consult Pick-up Orders
    SELECT

        od.order_country_code                                                           as country_code,
        case 
            when od.order_city_code = 'BCN' then 'BCN' 
            when od.order_city_code = 'MAD' then 'MAD' 
            else 'EXP' end                                                              as area_code,
        od.order_city_code                                                              as city_code,
        year (od.order_activated_local_at)                                              as year,
        month (od.order_activated_local_at)                                             as month,
        week (od.order_activated_local_at)                                              as week,
        DATE_FORMAT(od.order_activated_local_at, '%Y-%m-%d')                            as "date",  
        case when msa.warehouse_name = 'VALF2 - Maestro Guerrero'  then 'VALF2 - Centelles' else msa.warehouse_name end    as warehouse_name,
        COUNT(DISTINCT od.order_id )                                                    as pick_up_orders
    
    FROM delta.central_order_descriptors_odp.order_descriptors_v2 AS od
    
    LEFT JOIN
        delta.mfc_sales_odp.mfc_store_addresses_history AS msa 
            ON od.store_address_id = msa.store_address_id
            AND msa.valid_to >= CURRENT_DATE
    
    LEFT JOIN delta.central_geography_odp.cities_v2 AS cities 
        ON od.order_city_code = cities.city_code
    
    WHERE (od.order_final_status ) = 'DeliveredStatus' and (od.order_handling_strategy ) = 'PICKUP'
        AND od.order_activated_local_at >= (select rango_fecha from ranges)
        AND od.p_creation_date >= (select rango_fecha from ranges)
        AND (msa.warehouse_name IS NOT NULL OR msa.warehouse_business_id IS NOT NULL )
        AND (cities.country_code ) = 'ES'
    
    GROUP BY
        1,2,3,4,5,6,7,8
    ORDER BY
        1,2,3,4,5,6,7,8
)

    Select 
        o.country_code,
        o.area_code,
        o.city_code,
        o.year,
        o.month,
        o.week,
        o.date,
        o.warehouse_name,
        o.placed_orders,
        o.delivered_orders,
        o.n_orders_total_hard_cancellations,
        o."%_HC",
        o.n_orders_DT_more_60,
        o.remakes,
        gmv.total_orders,
        gmv.revenue_net_eur as gmv,
        sh.total_shrinkage_eur as shrinkage,
        ia.total_cost_of_inventory_regularization_eur as ia,
        fr.total_product_quantity_sold as fr,
        COALESCE((sh.total_shrinkage_eur + ia.total_cost_of_inventory_regularization_eur + (fr.total_product_quantity_sold * 5)) / (gmv.revenue_net_eur),0) as "fw_+_ir",
        br.num_order_br_uf, 
        br.ultra_fresh_delivered,
        br.percentage_br_uf,
        pna.percentage_of_orders_with_pna,
        pup.pick_up_orders,
        pna.total_order_pna,
        pna.n_pna_orders
    FROM orders as o

        LEFT JOIN gmv
            ON o.country_code = gmv.country_code AND o.area_code = gmv.area_code AND o.city_code = gmv.city_code
            AND o.year = gmv.year AND o.month = gmv.month AND o.week = gmv.week AND o.date = gmv.date
            AND o.warehouse_name = gmv.warehouse_name

        LEFT JOIN sh
            ON o.country_code = sh.country_code AND o.area_code = sh.area_code AND o.city_code = sh.city_code
            AND o.year = sh.year AND o.month = sh.month AND o.week = sh.week AND o.date = sh.date
            AND o.warehouse_name = sh.warehouse_name

        LEFT JOIN ia
            ON o.country_code = ia.country_code AND o.area_code = ia.area_code AND o.city_code = ia.city_code
            AND o.year = ia.year AND o.month = ia.month AND o.week = ia.week AND o.date = ia.date
            AND o.warehouse_name = ia.warehouse_name

        LEFT JOIN fr
            ON o.country_code = fr.country_code AND o.area_code = fr.area_code AND o.city_code = fr.city_code
            AND o.year = fr.year AND o.month = fr.month AND o.week = fr.week AND o.date = fr.date
            AND o.warehouse_name = fr.warehouse_name

        LEFT JOIN br
            ON o.country_code = br.country_code AND o.area_code = br.area_code AND o.city_code = br.city_code
            AND gmv.year = br.year AND gmv.month = br.month AND gmv.week = br.week AND gmv.date = br.date
            AND gmv.warehouse_name = br.warehouse_name

        LEFT JOIN pna
            ON o.country_code = pna.country_code AND o.area_code = pna.area_code AND o.city_code = pna.city_code
            AND o.year = pna.year AND o.month = pna.month AND o.week = pna.week AND o.date = pna.date
            AND o.warehouse_name = pna.warehouse_name

        LEFT JOIN pup
            ON o.country_code = pup.country_code AND o.area_code = pup.area_code AND o.city_code = pup.city_code
            AND o.year = pup.year AND o.month = pup.month AND o.week = pup.week AND o.date = pup.date
            AND o.warehouse_name = pup.warehouse_name

    order by 1,2,3,4,5,6,7
