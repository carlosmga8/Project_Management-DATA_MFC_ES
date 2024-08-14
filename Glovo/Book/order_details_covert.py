--Order details clasified by pd_tiers, op_eff, n_orders, n_couriers , dt_times

with filter as (select date('2024-06-10') as "start",
                       date('2024-06-16') as "end"  ),

 meters as (
    SELECT
        mfc_name,
        date(date_format(date(ol.order_started_local_at), '%Y-%m-%d')) as day,
        
        count(distinct ol.order_id) as n_orders,
        sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 0.49 then 1 else 0 end) as "PD_less_than_0_5km",
                (sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 0.49 then 1 else 0 end) / cast(count(distinct ol.order_id) as decimal(10,2))) * 100 as "0_to_0_5_km_%",
        avg(case 
            when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 0.49 
            then (DATE_DIFF('second', CAST(order_picked_up_by_courier_local_at AS TIMESTAMP), CAST(order_courier_arrival_to_delivery_local_at AS TIMESTAMP)) / 60.00)end) as "avg_dt_0_to_0_5_km",
        sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 0.5 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 0.99 then 1 else 0 end) as "PD_between_0_5km_and_1km",
        (sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 0.5 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 0.99 then 1 else 0 end) / cast(count(distinct ol.order_id) as decimal(10,2))) * 100 as "0_5_to_1_km_%",
        avg(case 
            when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 0.5 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 0.99 
            then (DATE_DIFF('second', CAST(order_picked_up_by_courier_local_at AS TIMESTAMP), CAST(order_courier_arrival_to_delivery_local_at AS TIMESTAMP)) / 60.00)end) as "avg_dt_0_5_to_1_km",
        sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 1.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 1.99 then 1 else 0 end) as "PD_between_1km_and_2km",
        (sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 1.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 1.99 then 1 else 0 end) / cast(count(distinct ol.order_id) as decimal(10,2))) * 100 as "1_to_2_km_%",
        avg(case 
            when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 1.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 1.99 
            then (DATE_DIFF('second', CAST(order_picked_up_by_courier_local_at AS TIMESTAMP), CAST(order_courier_arrival_to_delivery_local_at AS TIMESTAMP)) / 60.00)end) as "avg_dt_1_to_2_km",
        sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 2.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 2.99 then 1 else 0 end) as "PD_between_2km_and_3km",
        (sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 2.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 2.99 then 1 else 0 end) / cast(count(distinct ol.order_id) as decimal(10,2))) * 100 as "2_to_3_km_%",
        avg(case 
            when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 2.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 2.99 
            then (DATE_DIFF('second', CAST(order_picked_up_by_courier_local_at AS TIMESTAMP), CAST(order_courier_arrival_to_delivery_local_at AS TIMESTAMP)) / 60.00)end) as "avg_dt_2_to_3_km",
        sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 3.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 4.99 then 1 else 0 end) as "PD_between_3km_and_5km",
        (sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 3.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 4.99 then 1 else 0 end) / cast(count(distinct ol.order_id) as decimal(10,2))) * 100 as "3_to_5_km_%",
        avg(case 
            when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 3.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 4.99 
            then (DATE_DIFF('second', CAST(order_picked_up_by_courier_local_at AS TIMESTAMP), CAST(order_courier_arrival_to_delivery_local_at AS TIMESTAMP)) / 60.00)end) as "avg_dt_3_to_5_km",
        sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 5.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 20.0 then 1 else 0 end) as "PD_greater_than_5km",
        (sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 5.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 20.0 then 1 else 0 end) / cast(count(distinct ol.order_id) as decimal(10,2))) * 100 as ">5_km_%",
        avg(case 
            when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 5.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 20.0 
            then (DATE_DIFF('second', CAST(order_picked_up_by_courier_local_at AS TIMESTAMP), CAST(order_courier_arrival_to_delivery_local_at AS TIMESTAMP)) / 60.00)end) as "avg_dt_>5_km",
        avg(pickup_to_delivery_flight_distance_in_meters) / 1000 as avg_pd_km,
        avg(delivery_time_in_seconds)/60  as dt_minutes,
        avg(DATE_DIFF('second', CAST(order_picked_up_by_courier_local_at AS TIMESTAMP), CAST(order_courier_arrival_to_delivery_local_at AS TIMESTAMP)) / 60.00) AS "avg_minutes_pdt_ovl"
    FROM
        "delta"."courier__in_house_supply__odp"."driven_distance_order_level" ol
    LEFT JOIN "delta"."courier_order_flow_odp"."delivery_times_order_level_attributes" la
    ON ol.order_id = la.order_id
    LEFT JOIN "delta"."central_order_descriptors_odp"."order_descriptors_v2" od
    ON ol.order_id = od.order_id
    WHERE ol.order_started_local_at between (select "start" from filter) and (select "end" from filter) 
           
        and not is_canceled
    GROUP BY 1, 2
),
hours as (
    SELECT 
        mfc_name,
        city,
        reference_week,
        date(date_format(date(start_time), '%Y-%m-%d')) as day,
        
        sum(worked_hours_in_slot) as worked_hours,
        count(distinct courier_id) as n_couriers
    FROM
        "delta"."courier__in_house_supply__odp"."worked_minutes_per_slot" wm
    LEFT JOIN
        "delta"."courier__in_house_supply__odp"."store_addresses" sa
        ON wm.store_address_id = sa.store_address_id
    WHERE
        wm.store_address_id is not null
    GROUP BY 1, 2, 3, 4),

 over_dt_target as (
    SELECT
        mfc_name,
        date(date_format(date(ol.order_started_local_at), '%Y-%m-%d')) as day,
        
        sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 0.49 then 1 else 0 end) as "over_60_0_5km",

        sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 0.5 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 0.99 then 1 else 0 end) as "over_60_between_0_5km_and_1km",
        
        sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 1.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 1.99 then 1 else 0 end) as "over_60_between_1km_and_2km",
        
        sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 2.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 2.99 then 1 else 0 end) as "over_60_between_2km_and_3km",
        
        sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 3.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 4.99 then 1 else 0 end) as "over_60_between_3km_and_5km",

        sum(case when (pickup_to_delivery_flight_distance_in_meters / 1000) >= 5.0 and (pickup_to_delivery_flight_distance_in_meters / 1000) <= 20.0 then 1 else 0 end) as "over_60_greater_than_5km"

    FROM
        "delta"."courier__in_house_supply__odp"."driven_distance_order_level" ol
    LEFT JOIN "delta"."courier_order_flow_odp"."delivery_times_order_level_attributes" la
    ON ol.order_id = la.order_id
    LEFT JOIN "delta"."central_order_descriptors_odp"."order_descriptors_v2" od
    ON ol.order_id = od.order_id
    WHERE ol.order_started_local_at between (select "start" from filter) and (select "end" from filter) 
    AND (delivery_time_in_seconds / 60) >60    
    and not is_canceled
    GROUP BY 
    1,2
)
SELECT 
    m."mfc_name",
    h."city",
    EXTRACT(week FROM CAST(m.day AS DATE)) as "weeknum",
    m."day",
    
    m."n_orders",
    m."PD_less_than_0_5km",
        m."0_to_0_5_km_%",
        (odt."over_60_0_5km") as over_60_0_5km,
        60.00 / ((m."avg_dt_0_to_0_5_km"*2)+3.50) as "eff_0_to_0_5_km",
    m."PD_between_0_5km_and_1km",
        m."0_5_to_1_km_%",
        (odt."over_60_between_0_5km_and_1km") as over_60_between_0_5km_and_1km,
        60.00 / ((m."avg_dt_0_5_to_1_km"*2)+3.50) as "eff_0_5_to_1_km",
    m."PD_between_1km_and_2km",
        m."1_to_2_km_%",
        (odt."over_60_between_1km_and_2km") as over_60_between_1km_and_2km,
        60.00 / ((m."avg_dt_1_to_2_km"*2)+3.50) as "eff_1_to_2_km",
    m."PD_between_2km_and_3km",
        m."2_to_3_km_%",
        (odt."over_60_between_2km_and_3km") as over_60_between_2km_and_3km,
        60.00 / ((m."avg_dt_2_to_3_km"*2)+3.50) as "eff_2_to_3_km",
    m."PD_between_3km_and_5km",
        m."3_to_5_km_%",
        (odt."over_60_between_3km_and_5km") as over_60_between_3km_and_5km,
        60.00 / ((m."avg_dt_3_to_5_km"*2)+3.50) as "eff_3_to_5_km",
    m."PD_greater_than_5km",
        m.">5_km_%",
        (odt."over_60_greater_than_5km") as "over_60_greater_than_5km",
        60.00 / ((m."avg_dt_>5_km"*2)+3.50) as "eff_>5_km",
    round(m.avg_pd_km,2) as "avg_pd_km",
    h."worked_hours",
    h."n_couriers",
    round(dt_minutes,2) as "dt_minutes",
    m."avg_minutes_pdt_ovl",
    CASE
        WHEN h.worked_hours = 0 THEN NULL
        ELSE round(m.n_orders / h.worked_hours,2)
    END as "efficiency"
FROM
    meters m
LEFT JOIN
    hours h
    
    ON h.day = m.day
    AND h.mfc_name = m.mfc_name
LEFT JOIN
    over_dt_target odt
    
    ON odt.day = m.day
    AND odt.mfc_name = m.mfc_name
WHERE
    m.avg_pd_km is not null
    AND worked_hours is not null
    AND m.day between (select "start" from filter) and (select "end" from filter) 
    And h.city = 'BCN' 
    or h.city = 'MAD' 
    or h.city = 'VAL' 
    or h.city = 'SEV' 
    or h.city = 'MAL' 
    or h.city = 'ZAR' 
    or h.city = 'PAL'
    and case when(odt."over_60_0_5km") = null then (odt."over_60_0_5km" = 0) end
ORDER BY 1, 2, 3, 4
