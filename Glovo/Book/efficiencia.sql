with meters as (
SELECT
mfc_name,
ol.order_city_code,
year(ol.order_started_local_at) as year,
date_format(date(ol.order_started_local_at), '%Y-%m-%d') as day,
week_of_year(ol.order_started_local_at) as week,
hour(ol.order_started_local_at) as hour,
count(distinct ol.order_id) as n_orders,
avg(pickup_to_delivery_flight_distance_in_meters) / 1000 as "avg_pd_km",
avg(DATE_DIFF('second', CAST(order_picked_up_by_courier_local_at AS TIMESTAMP), CAST(order_courier_arrival_to_delivery_local_at AS TIMESTAMP)) / 60.00) as "avg_PD_Time",
avg(delivery_time_in_seconds)/60 as dt_minutes

FROM
"delta"."courier__in_house_supply__odp"."driven_distance_order_level" ol
LEFT JOIN "delta"."courier_order_flow_odp"."delivery_times_order_level_attributes" la
ON ol.order_id = la.order_id
LEFT JOIN "delta"."central_order_descriptors_odp"."order_descriptors_v2" od
ON ol.order_id = od.order_id
WHERE
not is_canceled
and ol.order_city_code = 'BCN'
GROUP BY 1, 2, 3,4,5,6
),


hours as (
SELECT
warehouse_name as mfc_name,
city,
reference_week,
date_format(date(start_time), '%Y-%m-%d') as day,
year(start_time) as year,
week_of_year(start_time) as week,
hour(start_time) as hour,
sum(worked_hours_in_slot) as worked_hours,
count(distinct courier_id) as n_couriers
FROM
"delta"."courier__in_house_supply__odp"."worked_minutes_per_slot" wm
LEFT JOIN
delta.mfc_sales_odp.mfc_store_addresses_history sa
ON  (current_date) between valid_from and valid_to 
and sa.warehouse_business_id is not null
and  sa.store_address_id = wm.store_address_id


WHERE
wm.store_address_id is not null and not wm.store_address_id = 556074
and city = 'BCN'
GROUP BY 1, 2, 3, 4,5,6,7
)
SELECT
m.mfc_name,
h.city,
EXTRACT(week FROM CAST(m.day AS DATE)) as weeknum,
m.day,
m.week,
m.hour,
m.n_orders,
round(m.avg_pd_km,2) avg_pd_km,
m.avg_PD_Time,
h.worked_hours,
h.n_couriers,
round(dt_minutes,2) dt_minutes,
CASE
WHEN h.worked_hours = 0 THEN NULL
ELSE round(m.n_orders / h.worked_hours,2)
END as efficiency
FROM
meters m
LEFT JOIN
hours h
ON h.week = m.week
AND h.day = m.day
and h.year = m.year
and h.hour = m.hour
AND h.mfc_name = m.mfc_name
WHERE
avg_pd_km is not null
AND worked_hours is not null
and h.year = 2024
AND h.week >= 36 
And h.city = 'BCN'
and m.mfc_name  <> 'BCNF4 - Marina' and h.mfc_name  <> 'BCNF4 - Marina'
ORDER BY 1, 2, 3, 4, 5, 6
