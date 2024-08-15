SELECT 
    date(date_format(date(start_time), '%Y-%m-%d')) as day,
    hour(start_time) as hours,
    store_address_id,
    courier_id,
    worked_hours_in_slot
FROM
    "delta"."courier__in_house_supply__odp"."worked_minutes_per_slot"
WHERE 
    store_address_id = 556074


    AND p_reference_date  between date('2024-07-01') and date('2024-07-31') 
Order by 1,2
