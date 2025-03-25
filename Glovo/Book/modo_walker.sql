
with lu as (SELECT  
max(creation_time) as creation_time,
actor_id,
subject_id,
fa.vehicle,
arguments      
FROM "delta"."contact_audit_logs_odp"."audit_log_entries" 
Left join "delta"."courier__in_house_supply__odp"."spreadsheet_in_house_fleet_attributes" fa
ON subject_id = cast(fa.courier_id as varchar)
where 
    subject= 'courier'
    and action = 'Edit primary settings'
    and enabled_in_admin = true
    and is_jdt = false
    and courier_type = 'IH'
group by 2,3,4,5) 

select
creation_time,
actor_id,
subject_id,
vehicle,
arguments,
case when arguments  LIKE '%WALKER%' then true else false end as walkers
from lu
order by 3
