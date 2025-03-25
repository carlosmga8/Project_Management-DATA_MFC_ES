SELECT


wh.city                                                    As city,
    case 
        when wh.city = 'BCN' then 'BCN' 
        when wh.city = 'MAD' then 'MAD' 
        else 'EXP' end                                  As area_code,

wh.year                                                    As year,
wh.month                                                   As month,
wh.week                                                    As Week,
date(date_format(date(wh.start_time), '%Y-%m-%d'))         As day,
wh.store_address_id                                        As store_id,
wh.courier_id                                              As employed_id,
fa.is_jdt,
is_courier_picker,
min(extract (hour from wh.start_time))                     As start_shift,
max(extract(hour from wh.finish_time))                     As finish_shift,
 case
        when extract (hour from wh.start_time) between 00 and 06 then 'Early morning shift'
        when extract (hour from wh.start_time) between 06 and 17 then 'Morning shift'
        when extract (hour from wh.start_time) >= 17  then 'Night shift'
        else null end                                   As Shift,
max(extract(hour from wh.finish_time)) - min(extract (hour from wh.start_time)) as hours_of_shift


FROM
delta."courier__in_house_supply__odp"."worked_minutes_per_slot" wh
Left Join "delta"."courier__in_house_supply__odp"."spreadsheet_in_house_fleet_attributes" fa
    On  wh.courier_id = fa.courier_id 
WHERE
store_address_id in (535481,556074,556075,556076,556078,556079,572044,585667,594000,606407,624286,642548,649201)
--AND p_reference_date  between date('2024-02-01') and date('2024-10-01')
and wh.month = 3
and wh.year = 2025
--AND courier_id in (86861203, 138158209,84595905,162971795)
AND wh.courier_id = 187608906
Group by 1,2,3,4,5,6,7,8,9,10,13
Order by 1,2,3,4,5,6,9
