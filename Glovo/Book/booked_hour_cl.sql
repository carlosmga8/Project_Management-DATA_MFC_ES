SELECT
    dl.courier_id  AS "dl.courier_id",
    week(dl.p_aggregation_date ) as week,
    
    dl.city_code  AS "dl.city_code",
    COALESCE(SUM(dl.no_show_time_in_minutes ), 0) AS "dl.no_show_time_in_minutes",
    COALESCE(SUM(dl.n_slots_booked ), 0)/2 AS "dl.n_slots_booked",
    COALESCE(SUM(dl.n_slots_no_show ), 0)/2 AS "dl.n_slots_no_show",
    COALESCE(SUM(dl.n_slots_checked_in ), 0)/2 AS "dl.n_slots_checked_in"
FROM delta.courier__courier_performance_analytics__odp.courier_performance_analytics_courier_day_level_components  AS dl
WHERE dl.p_aggregation_date >=  DATE_TRUNC('DAY', CURRENT_DATE) - INTERVAL '100' DAY

AND (dl.country_code ) = 'ES' AND (dl.business_model ) = 'EMPLOYEE'
    AND courier_id in (72341839,72342100,74712064,83170175,83323158,84527875,84551018,84594035,84595905,85379160,86861203,89583112,90019645,132323774,133278647,133443042,137570836,138158209,138157095,
    142069556,143369520,149453463,153581032,153690832,156272973,156712756,158263518,159711226,162517999,162971795,164318642,166434060,179552242)
GROUP BY 1, 2, 3
ORDER BY
    2 
