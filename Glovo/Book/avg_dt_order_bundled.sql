WITH RankedOrders AS (
    SELECT
        year(order_activated_local_at) AS year,
        month(order_activated_local_at) AS Month,
        week(order_activated_local_at) AS Week,
        ol.order_id,
        bundle_id,
        ol.num_orders_in_bundle AS num_bundled,

        CAST(order_terminated_local_at AS TIMESTAMP) AS order_terminated,

        COUNT(DISTINCT od.order_id) AS N_total_order_by_tier_bundled,
        
        COUNT(DISTINCT od.order_id) FILTER (
            WHERE DATE_DIFF('second', CAST(order_activated_local_at AS TIMESTAMP), CAST(order_terminated_local_at AS TIMESTAMP)) / 60.00 > 60.00
        ) AS "N_order_>60'_by_tier_bundled",

        COALESCE(1.00000 * COUNT(DISTINCT od.order_id) FILTER (
            WHERE DATE_DIFF('second', CAST(order_activated_local_at AS TIMESTAMP), CAST(order_terminated_local_at AS TIMESTAMP)) / 60.00 > 60.00
        ) / COUNT(DISTINCT od.order_id), 0) AS "%_order_>60'_by_tier",                

        ROUND(AVG(DATE_DIFF('second', CAST(order_activated_local_at AS TIMESTAMP), 
            CAST(order_terminated_local_at AS TIMESTAMP)) / 60.00), 2) AS avg_delivered_time_min

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
        AND ol.num_orders_in_bundle IN (2, 3, 4)
        AND week(order_activated_local_at) = 12

    GROUP BY 
        1,2,3,4,5,6,7
)

-- Ahora utilizamos una subconsulta para asignar la posición de entrega
, DeliveryPositions AS (
    SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY bundle_id ORDER BY order_terminated) AS delivery_position
    FROM 
        RankedOrders
)

-- Consulta final para calcular el promedio del tiempo de entrega por posición
SELECT 
    year,
    Month,
    Week,
    delivery_position,
    COUNT(order_id) AS order_count,
    AVG(avg_delivered_time_min) AS average_delivery_time_min
FROM 
    DeliveryPositions
GROUP BY 
    1, 2, 3, 4  -- Agrupación por año, mes, semana, y posición de entrega
ORDER BY 
    2, 3, 4;    -- Ordenando por mes, semana, y posición de entrega
