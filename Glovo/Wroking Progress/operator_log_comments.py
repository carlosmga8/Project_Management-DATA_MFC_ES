WITH filter AS (
    SELECT DATE('2024-01-01') AS start_date,
           DATE('2024-06-07') AS end_date
)

SELECT 
    order_city_code,
    DATE_FORMAT(creation_time, '%Y-%m-%d %H:%i:%s') AS "Date",
    ACTOR_ID AS operator,
    ACTION,
    subject_id AS order_id,
    arguments,
    round(AVG(delivery_time_in_seconds) / 60.00,2) AS dt_minutes
FROM 
    delta.contact_audit_logs_odp.audit_log_entries
LEFT JOIN 
    delta.courier_order_flow_odp.delivery_times_order_level_attributes 
    ON audit_log_entries.subject_id = CAST(delivery_times_order_level_attributes.order_id AS VARCHAR)
WHERE 
    ACTION = 'Add comment' 
    AND subject = 'order'
    AND ACTOR_ID = 159704216
    AND creation_time BETWEEN (SELECT start_date FROM filter) AND (SELECT end_date FROM filter)
    AND ((delivery_time_in_seconds) / 60.00)  >= 55.00
    and ((delivery_time_in_seconds) / 60.00) <= 62.00
group by 1,2,3,4,5,6
order by 2
