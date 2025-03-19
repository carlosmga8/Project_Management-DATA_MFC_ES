with cls as (SELECT
date(date_format(date(ws.start_time), '%Y-%m-%d')) as day,
ws.city,
ws.store_address_id,
ws.courier_id,
hour(start_time) as hours,
(ws.start_time) start_time,
(ws.finish_time) finish_time

FROM "delta"."courier__in_house_supply__odp"."worked_minutes_per_slot"  ws
LEFT JOIN "delta"."courier__in_house_supply__odp"."spreadsheet_in_house_fleet_attributes" fa
    ON ws.courier_id = fa.courier_id
WHERE
ws.store_address_id in (556074,556076,649201,556079,535481,556075)
AND ws.p_reference_date  between date('2024-01-01') and date('2025-01-01')
AND hour(start_time) in (0,1,2,3,4,5,6,7)
AND is_jdt = true
--GROUP BY 1,2,3,4,5,6
Order by 1,2,3,4,5,6),

Orders as (SELECT 
    extract(year from o.order_activated_local_at)                       as year,
    extract(month from o.order_activated_local_at)                      as month,
    date(date_format(date(o.order_activated_local_at), '%Y-%m-%d'))     as day,
    extract(hour from o.order_activated_local_at)                       as hour,
    msa.city_code                                                       as city,
    count( distinct  case  when  o.order_parent_relationship_type is null
                                then    o.order_id
                                else    null    end)                    as n_orders

    FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
    INNER JOIN delta.mfc_sales_odp.mfc_store_addresses_history msa
    ON  (current_date) between valid_from and valid_to 
    and msa.warehouse_business_id is not null
    and  msa.store_address_id = o.store_address_id

    WHERE o.p_creation_date between date('2024-01-01') and date('2025-01-01')
            and o.order_activated_local_at between date('2024-01-01') and date('2025-01-01')
            and msa.country_code = 'ES'
            AND (o.order_subvertical ) = 'MFC' 
            AND hour(o.order_activated_local_at) in (0,1,2,3,4,5,6,7)
    GROUP BY 1,2,3,4,5
    ORDER BY 1,2,3,4,5)

    SELECT 
        cl.day,  
        cl.city,
        cl.store_address_id,
        cl.courier_id,
        cl.hours,
        cl.start_time,
        cl.finish_time,
        o.n_orders
    from cls as cl
    Left Join Orders as o
        ON cl.day = o.day 
        and cl.city = o.city
        and cl.hours = o.hour
    Order by 1,2,4,5,6
