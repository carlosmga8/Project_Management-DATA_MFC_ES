SELECT 
    cb.courier_cash_balance_local_date as "Date",
    cb.city_code,
    cb.courier_id,
    cb.rolling_courier_cash_balance_eur as "cash_balance"
     
FROM "delta"."courier_cash_arrears_odp"."courier_cash_balance_snapshot" cb
left join 
    "delta"."courier__courier_performance_analytics__odp"."courier_performance_analytics_courier_day_level_components" 
        on courier_performance_analytics_courier_day_level_components.courier_id = cb.courier_id
where 
    cb.country_code = 'ES'
    and courier_performance_analytics_courier_day_level_components.business_model = 'EMPLOYEE'
    and cb.courier_id = 174138809 
    and cb.courier_cash_balance_local_date between date('2023-01-01') and  date(current_date)
    and cb.city_code not in ('BCN','MAD')
group by 1,2,3,4
order by 1 desc
