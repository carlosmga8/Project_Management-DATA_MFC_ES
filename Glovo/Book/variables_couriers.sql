with filter as (select date('2025-01-01') as "start",
                       date(current_date) as "end",--),
                       --date(current_date) as "end",
                       (189633328  ) as "courier_id"), 

                  
var as (SELECT 
courier_id,
year,
month,
--week,
full_week_contract_hours,
SUM(worked_hours) AS "W_HOURS",
SUM(night_worked_hours_payroll) AS "nights",
SUM(sunday_worked_hours_payroll) AS "sundays",
SUM(festive_worked_hours_payroll) AS "festives",
    CASE WHEN SUM(total_month_compl_hours) < 0 THEN 0 ELSE SUM(total_month_compl_hours) END AS "complements",
    CASE WHEN SUM(total_month_extra_hours) < 0 THEN 0 ELSE SUM(total_month_extra_hours) END AS "extra",
SUM(vacation_hours) AS "vacation_hours",
SUM(days_off_vacation) AS "days_off_vacation",
SUM(leaves_hours) AS "leaves_hours",
SUM(days_off_leaves) AS "days_off_leaves",
sum(billable_hours) as "billable_hours"
FROM
"delta"."courier__in_house_supply__odp"."worked_hours_per_week"
WHERE
p_reference_date between (select "start" from filter) and (select "end" from filter) 
and courier_id = (select "courier_id" from filter) --in (139784058,162587492,145519921,151253667,155407364)--
GROUP BY
1,2,3,4
Order by 
2),

tips as (SELECT
    EXTRACT(year FROM CAST(order_activated_local_at AS DATE)) as "year",
    EXTRACT(month FROM CAST(order_activated_local_at AS DATE)) as "month",
    --EXTRACT(week FROM CAST(order_activated_local_at AS DATE)) as "week",
    courier_tips.courier_id  AS "courier_id",
    round(SUM( courier_tips.tip_amount_eur),2) amount_in_eur 
FROM delta.central_order_descriptors_odp.order_descriptors_v2  AS order_descriptors_v2
LEFT JOIN delta.courier_tipping_odp.courier_tipping  AS courier_tips ON order_descriptors_v2.order_id = courier_tips.order_id
WHERE (order_descriptors_v2.order_activated_local_at) between (select "start" from filter) and (select "end" from filter) 
AND (order_descriptors_v2.order_subvertical ) = 'MFC' 
AND (courier_tips.courier_id ) = (select "courier_id" from filter)--in (139784058,162587492,145519921,151253667,155407364)-- (select "courier_id" from filter) 
GROUP BY
    1,2,3
ORDER BY
    2 DESC),

meters as (SELECT
    EXTRACT(year FROM CAST(p_start_date AS DATE)) as "year",
    EXTRACT(month FROM CAST(p_start_date AS DATE)) as "month",
    --EXTRACT(week FROM CAST(p_start_date AS DATE)) as "week",
    courier_id,
    order_city_code as city,
    round(sum(paid_distance_in_meters)/1000,2) as "paid_distance_km"
FROM  
    "delta"."courier__in_house_supply__odp"."driven_distance_order_level"
WHERE 
    p_start_date between (select "start" from filter) and (select "end" from filter)
    and courier_id = (select "courier_id" from filter)--in (139784058,162587492,145519921,151253667,155407364)-- (select "courier_id" from filter) 
Group by
    1,2,3,4)

SELECT 
v.courier_id,
m.city,
m.year,
v.month,
--v.week,
v.full_week_contract_hours,
v.W_HOURS as m_hours,
v.nights,
v.sundays,
v.festives,
v.complements,
v.extra,
v.vacation_hours,
v.days_off_vacation,
v.leaves_hours,
v.days_off_leaves,
v.billable_hours,
round(t.amount_in_eur,2) as tips,
m.paid_distance_km as Km
FROM var v
Left join tips t 
ON v.courier_id = t.courier_id
and v.year = t.year
and v.month = t.month --and v.week = t.week
Left Join meters m
ON v.courier_id = m.courier_id
and v.year = m.year
and v.month = m.month
--and v.week = m.week
Order by 1,3,4,5
