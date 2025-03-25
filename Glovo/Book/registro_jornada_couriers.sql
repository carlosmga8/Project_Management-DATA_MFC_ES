SELECT
ss.scheduling_slot_city_code city_code,
warehouse_name,
ls.courier_id,
Case 
    When cast(DATE_FORMAT( DATE_ADD('hour', 2, scheduling_slot_courier_checked_in_at), '%Y-%m-%d')as date) < date('2024-10-27') 
    then DATE_FORMAT( DATE_ADD('hour', 2, scheduling_slot_courier_checked_in_at), '%Y-%m-%d') 
    else DATE_FORMAT( DATE_ADD('hour', 1, scheduling_slot_courier_checked_in_at), '%Y-%m-%d') end AS day,

--DATE_FORMAT( DATE_ADD('hour', 2, scheduling_slot_courier_checked_in_at), '%Y-%m-%d ') AS day,  --este cambio incluye el cambio de hora de verano del 27 de octubre.

Case 
    When (DATE_ADD('hour', 2, scheduling_slot_courier_checked_in_at) < TIMESTAMP '2024-10-27 00:00:00')
    Then DATE_FORMAT((DATE_ADD('hour', 2, scheduling_slot_courier_checked_in_at)), '%Y-%m-%d %H:%i:%s')
    Else DATE_FORMAT((DATE_ADD('hour', 1, scheduling_slot_courier_checked_in_at)), '%Y-%m-%d %H:%i:%s') end AS checkin,


--DATE_FORMAT((DATE_ADD('hour', 2, scheduling_slot_courier_checked_in_at)), '%Y-%m-%d %H:%i:%s') AS checkin, --este cambio incluye el cambio de hora de verano del 27 de octubre.

DATE_FORMAT(MIN(DATE_TRUNC('MINUTE', scheduling_slot_started_local_at)), '%Y-%m-%d %H:%i:%s')  AS shift_started,
DATE_FORMAT(MAX(DATE_TRUNC('MINUTE', scheduling_slot_finished_local_at)), '%Y-%m-%d %H:%i:%s')  AS shift_finished,
DATE_DIFF('MINUTE', MIN(DATE_TRUNC('MINUTE', scheduling_slot_started_local_at)), MAX(DATE_TRUNC('MINUTE', scheduling_slot_finished_local_at)))*1.0/60 AS hour_shift

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

scheduling_slot_courier_checked_in_at BETWEEN DATE('2025-02-17') AND DATE('2025-02-24') 
--and ss.scheduling_slot_city_code = 'MAD'
and warehouse_name = 'BCNF2 - Consell de Cent'

--AND ls.courier_id = 137122335
and scheduling_slot_started_local_at is not null
--and c.is_active = true
GROUP BY
1,2,3,4,5
ORDER BY
4,1,2,3,6
