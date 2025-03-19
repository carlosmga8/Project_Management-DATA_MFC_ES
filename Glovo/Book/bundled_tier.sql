SELECT DISTINCT 
    year(order_activated_local_at) AS year,
    month(order_activated_local_at) AS Month,
    --week(order_activated_local_at) AS Week,    
    --date(date_format(date(order_activated_local_at), '%Y-%m-%d')) AS day,
    sa.city_code AS city,
    CASE    
        WHEN sa.city_code = 'BCN' THEN 'BCN'
        WHEN sa.city_code = 'MAD' THEN 'MAD'
        ELSE 'EXP' 
    END AS shift_area,
    --sa.warehouse_name AS mfc_name,

    ol.num_orders_in_bundle AS num_bundled,

    COUNT(DISTINCT od.order_id) AS N_total_order_by_tier_bundled,


    COUNT(DISTINCT od.order_id) FILTER (
        WHERE DATE_DIFF('second', CAST(order_activated_local_at AS TIMESTAMP), CAST(order_terminated_local_at AS TIMESTAMP)) / 60.00 > 60.00
    ) AS "N_order_>60'_by_tier_bundled",

COALESCE(1.00000* COUNT(DISTINCT od.order_id) FILTER (
        WHERE DATE_DIFF('second', CAST(order_activated_local_at AS TIMESTAMP), CAST(order_terminated_local_at AS TIMESTAMP)) / 60.00 > 60.00
    ) /     COUNT(DISTINCT od.order_id),0) as "%_order_>60'_by_tier",                 



    ROUND(AVG(DATE_DIFF('second', CAST(order_activated_local_at AS TIMESTAMP), 
        CAST(order_terminated_local_at AS TIMESTAMP)) / 60.00), 2) AS "avg_delivered_time_(min)"

FROM 
    "delta"."central_order_descriptors_odp"."order_descriptors_v2" od

LEFT JOIN 
    "delta"."courier__in_house_supply__odp"."driven_distance_order_level" ol
    ON ol.order_id = od.order_id
    AND ol.courier_id = od.courier_id

INNER JOIN 
    delta.mfc_sales_odp.mfc_store_addresses_history sa
    ON (current_date) BETWEEN valid_from AND valid_to 
    AND sa.warehouse_business_id IS NOT NULL
    AND sa.store_address_id = od.store_address_id

WHERE 
    order_activated_local_at >= date('2025-01-01')
    AND sa.country_code = 'ES'
    AND (od.order_handling_strategy) <> 'PICKUP' 
    AND (od.order_handling_strategy) <> 'GEN1'  
    AND ol.bundle_id IS NOT NULL
    and  ol.num_orders_in_bundle in (2,3,4)
GROUP BY 
    1,2,3,4,5
ORDER BY 
   2,3,5
