
SELECT  distinct
cb.Courier_id,
cb.city_code,
year(cb.courier_account_entry_creation_local_datetime) as year,
month(cb.courier_account_entry_creation_local_datetime) as month,
DATE_FORMAT(cb.courier_account_entry_creation_local_datetime, '%Y-%m-%d')             as date,
cb.courier_account_entry_event_type,
cb.courier_account_entry_amount_local_currency

FROM "delta"."courier_cash_arrears_odp"."courier_cash_balance_detail" cb
left join 
    "delta"."courier__courier_performance_analytics__odp"."courier_performance_analytics_courier_day_level_components" lc
        on lc.courier_id = cb.courier_id
Left Join 
    "delta"."courier__in_house_supply__odp"."spreadsheet_in_house_fleet_attributes" fa
        on fa.courier_id = cb.courier_id

Where cb.country_code= 'ES' 
--and courier_account_entry_event_type = 'ORDER_PAID_CASH' 
and courier_account_entry_event_type = 'CASH_COLLECTED_FROM_COURIER'
and lc.business_model = 'EMPLOYEE'
and cb.city_code not in ('BCN','MAD')
and year(cb.courier_account_entry_creation_local_datetime) = 2025
order by 5
