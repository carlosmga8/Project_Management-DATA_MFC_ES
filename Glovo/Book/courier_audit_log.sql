SELECT distinct   
p_creation_day,
p_creation_hour,
subject
action,
arguments,
subject_id 
FROM "delta"."contact_audit_logs_odp"."audit_log_entries" 
where 
subject = 'courier'
and action in ('Edit secondary settings','Edit primary settings','Edit settings')
and actor_id = 159711304
order by 1 desc
