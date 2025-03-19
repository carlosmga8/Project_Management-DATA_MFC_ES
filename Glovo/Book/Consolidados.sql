with filter as (select date('2025-01-20') as "start",
                       --date('2024-06-30') as "end"),
                       date(current_date) as "end"),
                       
rj as (SELECT
ss.scheduling_slot_city_code                                                                                                        as city_code,
warehouse_name                                                                                                                      as MFC,
ls.courier_id                                                                                                                       as courier_id,
DATE_FORMAT( scheduling_slot_started_local_at, '%Y-%m-%d')                                                                          AS day,
count (distinct ls.scheduling_slot_id)   as n_slot,
concat(cast(Min(extract (hour from scheduling_slot_started_local_at))as varchar), ':00')                                            As "shift_Start",
    concat(cast((case
    when (Max(extract (hour from scheduling_slot_started_local_at)) +1) = 24
    then 0 else Max(extract (hour from scheduling_slot_started_local_at)) +1 end) as varchar),':00')                                As "shift_Finish",

--DATE_DIFF('MINUTE', MIN(DATE_TRUNC('MINUTE', scheduling_slot_started_local_at)), 
--MAX(DATE_TRUNC('MINUTE', scheduling_slot_finished_local_at)))*1.0/60        
    (Max(extract (hour from scheduling_slot_started_local_at)) +1)   -                                                    
    Min(extract (hour from scheduling_slot_started_local_at))                                                                        AS hour_shift,
 case
        when (case
    when ((extract (hour from scheduling_slot_started_local_at)) +1) = 24
    then 0 else (extract (hour from scheduling_slot_started_local_at)) +1 end) between 00 and 06 then 'early morning'
        when (case
    when ((extract (hour from scheduling_slot_started_local_at)) +1) = 24
    then 0 else (extract (hour from scheduling_slot_started_local_at)) +1 end) between 06 and 16.0 then 'day shift'
        when (case
    when ((extract (hour from scheduling_slot_started_local_at)) +1) = 24
    then 0 else (extract (hour from scheduling_slot_started_local_at)) +1 end) >= 16  then 'night shift'
        else null end                                                                                                               As Shift,

--CASE
 DAY_OF_WEEK(scheduling_slot_started_local_at)
--WHEN 7 THEN 'Sunday'
--WHEN 1 THEN 'Monday'
--WHEN 2 THEN 'Tuesday'
--WHEN 3 THEN 'Wednesday'
--WHEN 4 THEN 'Thursday'
--WHEN 5 THEN 'Friday'
--WHEN 6 THEN 'Saturday'
--END                                                                                                                                 
as day_of_the_week

FROM
delta."courier_logistics_scheduling_odp"."scheduling_slots_couriers" ls

INNER JOIN delta.mfc_sales_odp.mfc_store_addresses_history sa
    ON  (current_date) between valid_from and valid_to 
    and sa.warehouse_business_id is not null
    and  sa.store_address_id = ls.store_address_id


LEFT JOIN
delta."courier_logistics_scheduling_odp"."scheduling_slots" ss
ON
ss.scheduling_slot_id = ls.scheduling_slot_id
AND ss.scheduling_slot_city_code = sa.city_code
INNER JOIN "delta"."courier__in_house_supply__odp"."spreadsheet_in_house_fleet_attributes" c
ON ls.courier_id = c.courier_id
WHERE

scheduling_slot_courier_checked_in_at between (select "start" from filter) and (select "end" from filter) 
and ss.scheduling_slot_city_code = 'BCN'
and scheduling_slot_started_local_at is not null
and c.is_active = true
GROUP BY
1,2,3,4,9,10
ORDER BY
4,1,2,3,6),

orders as (
SELECT
    --msa.warehouse_name  AS mfc_name,
    od.courier_id  AS courier_id,
    COUNT(DISTINCT od.order_id ) AS n_orders
FROM delta.central_order_descriptors_odp.order_descriptors_v2  AS od
LEFT JOIN delta.mfc_sales_odp.mfc_store_addresses_history  AS msa ON od.store_address_id = msa.store_address_id
      AND msa.valid_to > od.p_creation_date
      AND msa.valid_from <= od.p_creation_date
WHERE  ((od.order_final_status ) = 'DeliveredStatus' AND ((od.order_handling_strategy ) <> 'PICKUP' 
AND (od.order_handling_strategy ) <> 'GEN1' OR (od.order_handling_strategy ) IS NULL)) 
AND  od.order_activated_local_at   BETWEEN (select "start" from filter) and (select "end" from filter)  
AND  od.order_created_local_at   BETWEEN (select "start" from filter) and (select "end" from filter) 
AND od.p_creation_date  BETWEEN (select "start" from filter) and (select "end" from filter) 
AND msa.store_address_id IS NOT NULL 
GROUP BY
    1  )


Select  
rj.city_code,
fa.MFC,
rj.Courier_ID,
fa.contract_hours,
fa.hiring_date,
fa.vehicle,
o.n_orders,
Count (distinct day) as worked_days,
sum(n_slot) / 2.00 as Booked_Hours,
Count (distinct day) * (cast(fa.contract_hours as decimal) / 5.00) as theoretical_hourss, 
COUNT(DISTINCT case when Shift = 'early morning' then day end) as n_early_morning_shift,
COUNT(DISTINCT case when Shift = 'day shift' then day end) as n_day_shift,
COUNT(DISTINCT case when Shift = 'night shift' then day  end) as n_night_shift,
COUNT(DISTINCT case when day_of_the_week = 6 then day end) as n_saturdays,
COUNT(DISTINCT CASE WHEN day_of_the_week = 7 THEN day END) AS n_sundays,
case when COUNT(DISTINCT case when Shift = 'early morning' then day end) + COUNT(DISTINCT case when Shift = 'night shift' then day  end) > 
COUNT(DISTINCT case when Shift = 'day shift' then day end) then 'night shift' else 'day shift' end as predominant_shift,
o.n_orders / (sum(n_slot) / 2.00 ) as Efficiency
From rj
Left Join "delta"."courier__in_house_supply__odp"."spreadsheet_in_house_fleet_attributes" fa
on rj.courier_id = fa.courier_id
Left Join orders as o
On rj.courier_id = o.courier_id
Where
is_active = true
and is_jdt = false

Group by 1,2,3,4,5,6,7
Order by 1,2,3
