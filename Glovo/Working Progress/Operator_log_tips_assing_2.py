with filter as (select date('2024-01-01') as "start",
 date('2024-05-31') as "end" ),
 
operator as (SELECT 
    creation_time AS "Date_time",
    ACTOR_ID AS "Operator",
    ACTION,
    subject_id AS "order_id"
FROM 
    delta.contact_audit_logs_odp.audit_log_entries 
WHERE 
   
    subject = 'order'
    and not ACTION = 'ATS - ticket assigned'
    and not ACTION = 'ATS - ticket resolved'
    and not ACTION = 'Update manual constraints'
    and not ACTION = 'Validate orphan'),
    
tips as (SELECT
    (DATE_FORMAT(order_descriptors_v2.order_activated_local_at , '%Y-%m-%d')) AS "order_descriptors_v2.order_activated_local_date",
    courier_tips.courier_id  AS "courier_id",
    courier_tips.order_id  AS "order_id",
    COALESCE(SUM(( courier_tips.tip_amount  /power(10, order_descriptors_v2.order_currency_digits  )) *
      COALESCE( order_descriptors_v2.order_exchange_rate_to_eur  ,1) ), 0) AS "amount_in_eur"
FROM delta.central_order_descriptors_odp.order_descriptors_v2  AS order_descriptors_v2
LEFT JOIN delta.central_courier_tips_odp.courier_tips  AS courier_tips ON order_descriptors_v2.order_id = courier_tips.order_id
WHERE ((( order_descriptors_v2.order_activated_local_at  ) >= ((TIMESTAMP '2024-01-01')) AND ( order_descriptors_v2.order_activated_local_at  ) < ((DATE_ADD('year', 1, TIMESTAMP '2024-01-01'))))) AND (order_descriptors_v2.order_subvertical ) = 'MFC' 
GROUP BY
    1,
    2,
    3
ORDER BY
    3 DESC)

SELECT 
    DATE_FORMAT(o.Date_time, '%Y-%m-%d %H:%i:%s') AS "Date",
    o.Operator AS "Operator",
    o.order_id AS "Order_ID",
    o.ACTION,
    t.courier_id, 
    t.amount_in_eur
FROM 
    operator o
LEFT JOIN
    tips t
ON  
    CAST(o.order_id AS BIGINT) = t.order_id
    WHERE 
    
    
 O.Operator = 159704216
 and t.amount_in_eur > 0
 
    AND o.Date_time between (select "start" from filter) and (select "end" from filter) 
GROUP BY 
    1, 2, 3, 4, 5, 6
ORDER BY 
    1, 6
