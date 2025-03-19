        -- deliveries detail 
        
with filter as (select date('2024-09-02') as "start",
                       date(current_date) as "end")
                    

SELECT DISTINCT 
    year(order_activated_local_at)                                                                                                    as year,
    month(order_activated_local_at)                                                                                                   as Month,
    week(order_activated_local_at)                                                                                                    as Week,    
    date(date_format(date(order_activated_local_at), '%Y-%m-%d'))                                                                     as day,
    sa.city_code                                                                                                                      as city,
    CASE    when sa.city_code = 'BCN' Then 'BCN'
            when sa.city_code = 'MAD' Then 'MAD'

            else 'EXP' end                                                                                                           as shift_area,
    sa.warehouse_name                                                                                                               as mfc_name,
    CASE WHEN od.courier_id = 0 THEN NULL ELSE od.courier_id END                                                                    as courier_id,

    od.order_id                                                                                                                     as order_id,
    ol.bundle_id                                                                                                                    as bundle_id,
    ol.is_unbundled                                                                                                                 as is_unbundled,
    od.order_final_status                                                                                                           as final_status,
    ol.num_orders_in_bundle                                                                                                         as num_bundled,
    ol.is_canceled                                                                                                                  as canceled,
    od.order_cancel_reason                                                                                                          as cancel_reason,
    ROUND((DATE_DIFF('second', CAST(order_activated_local_at AS TIMESTAMP), 
        CAST(order_terminated_local_at AS TIMESTAMP)) / 60.00), 2)                                                                  as "delivered_time_(min)",

    CASE WHEN od.order_parent_relationship_type IS NULL THEN 'single_order' 
        ELSE od.order_parent_relationship_type END                                                                                  as order_type,
    count (distinct od.order_id)                                                                                                    as n_order,
    ol.position_at_order_termination        

FROM 
    "delta"."central_order_descriptors_odp"."order_descriptors_v2" od

LEFT JOIN 
    "delta"."courier__in_house_supply__odp"."driven_distance_order_level" ol
    ON ol.order_id = od.order_id
    AND ol.courier_id = od.courier_id

INNER JOIN delta.mfc_sales_odp.mfc_store_addresses_history sa
    ON  (current_date) between valid_from and valid_to 
    and sa.warehouse_business_id is not null
    and  sa.store_address_id = od.store_address_id



WHERE 
    --order_activated_local_at BETWEEN (select "start" from filter) and (select "end" from filter)
    order_activated_local_at >= date('2025-02-15')
    And sa.country_code = 'ES'
    AND (od.order_handling_strategy) <> 'PICKUP' 
    AND (od.order_handling_strategy) <> 'GEN1'  
    AND ol.bundle_id is not null
    AND     ROUND((DATE_DIFF('second', CAST(order_activated_local_at AS TIMESTAMP), 
        CAST(order_terminated_local_at AS TIMESTAMP)) / 60.00), 2) > 60.0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,19
ORDER BY 4,5,7
