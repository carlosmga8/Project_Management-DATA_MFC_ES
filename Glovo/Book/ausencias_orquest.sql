SELECT 
    product_id,
    employee_id,
    incidence_type_name,
    date_series AS incidence_from_date,
    date_series AS incidence_to_date,
    incidence_init_datetime,
    incidence_end_datetime,
    worked_minutes,
    incidence_type_is_full_day
FROM (
    SELECT 
        product_id,
        employee_id,
        incidence_type_name,
        incidence_init_datetime,
        incidence_end_datetime,
        worked_minutes,
        incidence_type_is_full_day,
        sequence(incidence_from_date, incidence_to_date, interval '1' day) AS date_series_list
    FROM 
        "sensitive_delta"."mfc_picker_management_odp"."incidences"
    WHERE 
        business_id = 'GES' 
        AND product_id = '15-C' 
        AND incidence_from_date >= date('2025-03-01') 
        --AND employee_id = '147924162'
) AS incidence_dates
CROSS JOIN UNNEST(date_series_list) AS t(date_series)
