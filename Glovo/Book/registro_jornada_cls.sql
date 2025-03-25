With filter as (select date('2025-01-01') as "start",
                       date('2026-01-01') as "end",--),
                       --date(current_date) as "end",
                       ('34340768C') as "nif"), 
                       --('BCNF1 - Girona') as "mfc"), 

 Day as (select 
    whp.country_code                                            As country,
    whp.city_code                                               As city,
    case 
        when whp.city_code = 'BCN' then 'BCN' 
        when whp.city_code = 'MAD' then 'MAD' 
        else 'EXP' end                                          As area_code,
    extract (year from whp.shift_date)                          As year,
    extract (month from whp.shift_date)                         As month,
    extract (week from whp.shift_date)                          As week,
    (DATE_FORMAT(whp.shift_date , '%Y-%m-%d'))                  As "date",
    msa.warehouse_name                                          As "warehouse_name",
    whp.employee_id                                             As "ID",
    CONCAT(whp.employee_surname, ' ', whp.employee_name)        As full_name,   
    (Min(whp.shift_hour))                                       As "shift_Start",
    case when (Max(whp.shift_hour) +1) = 24
    then 0 else (Max(whp.shift_hour) +1) end                    As "shift_Finish",
    (Max(whp.shift_hour) +1) - (Min(whp.shift_hour))            As "Shift_hours"

FROM sensitive_delta.sensitive_mfc__mfc_labor_workedhoursdist__odp.mfc_labor_workedhoursdist_by_hour  AS whp
LEFT JOIN delta.mfc_sales_odp.mfc_store_addresses_history  AS msa ON whp.mfc_business_id = msa.warehouse_business_id
        AND DATE((DATE_FORMAT(msa.valid_to , '%Y-%m-%d'))) >= CURRENT_DATE

WHERE (UPPER(whp.country_code) ) = 'ES' 
    AND ( whp.shift_date  ) between (select "start" from filter) and (select "end" from filter) 
    --and whp.employee_id = (select "nif" from filter)
    --and msa.warehouse_name =  (select "mfc" from filter)
    --and whp.shift_hour >5

GROUP BY
    1,2,3,4,5,6,7,8,9,10
),

Night as (select 
    whp.country_code                                            As country,
    whp.city_code                                               As city,
    case 
        when whp.city_code = 'BCN' then 'BCN' 
        when whp.city_code = 'MAD' then 'MAD' 
        else 'EXP' end                                          As area_code,
    extract (year from whp.shift_date)                          As year,
    extract (month from whp.shift_date)                         As month,
    extract (week from whp.shift_date)                          As week,
    (DATE_FORMAT(whp.shift_date , '%Y-%m-%d'))                  As "date",
    msa.warehouse_name                                          As "warehouse_name",
    whp.employee_id                                             As "ID",
    CONCAT(whp.employee_surname, ' ', whp.employee_name)        As full_name,    
    (Min(whp.shift_hour))                                       As "shift_Start",
    case when (Max(whp.shift_hour) +1) = 24
    then 0 else (Max(whp.shift_hour) +1) end                    As "shift_Finish",
    (Max(whp.shift_hour) +1) - (Min(whp.shift_hour))            As "Shift_hours"

FROM sensitive_delta.sensitive_mfc__mfc_labor_workedhoursdist__odp.mfc_labor_workedhoursdist_by_hour  AS whp
LEFT JOIN delta.mfc_sales_odp.mfc_store_addresses_history  AS msa 
ON whp.mfc_business_id = msa.warehouse_business_id
        AND DATE((DATE_FORMAT(msa.valid_to , '%Y-%m-%d'))) >= CURRENT_DATE

WHERE (UPPER(whp.country_code) ) = 'ES' 
    AND ( whp.shift_date  )between (select "start" from filter) and (select "end" from filter)
    --and whp.employee_id = (select "nif" from filter) 
    --and msa.warehouse_name = (select "mfc" from filter)
    --and whp.shift_hour Between 0 and 5

GROUP BY
    1,2,3,4,5,6,7,8,9,10
)

SELECT 
    country,
    city,
    area_code,
    year,
    month,
    week,
    date,
    warehouse_name,
    ID as NIF,
    full_name,
    CONCAT(CAST(shift_Start AS VARCHAR), ':00') AS shift_Start,
    CONCAT(CAST(shift_Finish AS VARCHAR), ':00') AS shift_Finish,
    Shift_hours

FROM 
    Day

UNION ALL

SELECT 
    country,
    city,
    area_code,
    year,
    month,
    week,
    date,
    warehouse_name,
    ID as NIF,
    full_name,
    CONCAT(CAST(shift_Start AS VARCHAR), ':00') AS shift_Start,
    CONCAT(CAST(shift_Finish AS VARCHAR), ':00') AS shift_Finish,
    Shift_hours

FROM 
    Night

ORDER BY 
    1,2,3,4,5,6,7,8,9,11
