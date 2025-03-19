SELECT
    lc.courier_id  AS "lc.courier_id",
    year (lc.p_aggregation_date) AS "p_aggregation_date_year",
    --lc.city_code  AS "city_code",
    COALESCE(SUM(lc.no_show_time_in_minutes ), 0) AS "no_show_time_in_minutes",
    COALESCE(SUM(lc.no_show_time_in_minutes ), 0) / 60.00 AS "no_show_time_in_hours",
    COALESCE(SUM(lc.n_slots_booked ), 0) AS "n_slots_booked",
    (SUM(lc.n_slots_booked ) / 2.00)  "Booked_in_(Hour)",
    COALESCE(SUM(lc.n_slots_no_show ), 0) AS "n_slots_no_show",
    COALESCE(SUM(lc.n_slots_checked_in ), 0) AS "n_slots_checked_in",
    ((SUM(lc.n_slots_booked ) * 30.00) - COALESCE(SUM(lc.no_show_time_in_minutes ), 0))/60.00  "Hour_checked",
    COALESCE(SUM(lc.no_show_time_in_minutes ), 0) / (SUM(lc.n_slots_booked ) * 30.00)    "%_time_lost"    
FROM delta.courier__courier_performance_analytics__odp.courier_performance_analytics_courier_day_level_components  AS lc
LEFT JOIN "delta"."courier__in_house_supply__odp"."spreadsheet_in_house_fleet_attributes" as fa
ON lc.courier_id = fa.courier_id
WHERE 

year(lc.p_aggregation_date)  = 2024
AND (lc.country_code ) = 'ES' 
AND (lc.business_model ) = 'EMPLOYEE'
and     lc.n_slots_booked >0
and lc.city_code = 'MAD'
and is_active = true 
and enabled_in_admin = true
GROUP BY
    1,2
ORDER BY
    3
