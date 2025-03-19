                --Query to Masterplan Cls
 with mfc as (select 

    year( o.order_activated_local_at)                                                                       as year,
    month(o.order_activated_local_at)                                                                       as month,
    week(o.order_activated_local_at)                                                                        as week,
    DATE_FORMAT(o.order_activated_local_at, '%Y-%m-%d')                                                     as date,
    hour(o.order_activated_local_at)                                                                        as hour,
    msa.city_code                                                                                           as city,
    msa.warehouse_name                                                                                      as warehouse_name,
    count(distinct case when o.order_parent_relationship_type is null 
        then o.order_id else null end)                                                                      as total_orders,
    count( distinct  case  when o.order_final_status = 'DeliveredStatus' 
            and o.order_parent_relationship_type is null
        then o.order_id else null end)                                                                      as orders_delivered, 
    count(distinct case when o.order_final_status = 'CanceledStatus'
            AND o.order_parent_id is null
            and not ((date_diff('second', o.order_created_local_at, o.order_terminated_local_at) 
                / 60.0 <= 5 -- cancel_time_before_5min_of_creation
            or (date_diff('second', o.order_activated_local_at, o.order_terminated_local_at) 
                / 60.0 <= 5 -- cancel_time_before_5min_of_activation
            and o.order_scheduled_local_at is not null) -- order_is_scheduled
            or (o.order_scheduled_local_at is not null
            and o.order_activated_local_at is null)) -- order_is_scheduled_but_not_activated
            and (o.order_cancel_reason is null
            or o.order_cancel_reason in ('SELF_CANCELLATION','UNKNOWN','OTHER',
            'DELIVERY_TAKING_TOO_LONG', 'COURIER_NOT_ASSIGNED', 
            'CUSTOMER_DOESNT_WANT_PRODUCTS'))) -- order_cancel_reason is null or in OPS reasons
            then o.order_id else null end)                                                                  as total_hard_cancellations,
    count(distinct case when o.order_final_status = 'CanceledStatus'
            and o.order_parent_id is null
            and o.order_cancel_reason not in ('CUSTOMER_ABSENT', 'SELF_CANCELLATION')
            and hour(o.order_created_local_at) in (23,0,1,2,3,4)
            then o.order_id else null end)                                                                  as total_night_cancellations,
    count(distinct case when o.order_final_status = 'DeliveredStatus'
            and o.order_parent_relationship_type is null
            and dta.delivery_time_in_seconds >= 60 * 60 then o.order_id
            else null end)                                                                                  as DT_more_60,
    count(distinct case when o.order_final_status = 'DeliveredStatus'
            and o.order_parent_relationship_type is not null then o.order_id
            else null end)                                                                                  as remakes
 
    FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
    INNER JOIN delta.mfc_sales_odp.mfc_store_addresses_history msa
        ON  (current_date) between valid_from and valid_to 
        and msa.warehouse_business_id is not null
        and  msa.store_address_id = o.store_address_id
    LEFT JOIN delta.courier_order_flow_odp.delivery_times_order_level_attributes as dta 
        on dta.order_id = o.order_id
    WHERE o.p_creation_date >=  date('2024-01-01') 
            and o.order_activated_local_at >= date('2024-01-01')
            and msa.country_code = 'ES'
    GROUP BY 1,2,3,4,5,6,7 
),  

cl as (SELECT 

    year(creation_time)                                                                                     as year,
    month(creation_time)                                                                                    as month,
    week(creation_time)                                                                                     as week,
    DATE_FORMAT((date_add('hour',1,creation_time)), '%Y-%m-%d')                                             as date,
    hour(date_add('hour',1,creation_time))                                                                  as hour,
    o.order_city_code                                                                                       as city,
    ACTOR_ID                                                                                                as operator,
    sa.warehouse_name                                                                                       as warehouse_name,
    count (distinct subject_id)                                                                             as n_orders,
    sum(case when action = 'Assign' then 1 else 0 end)                                                      as n_assing,    
    sum(case when action = 'Reassign' then 1 else 0 end)                                                    as n_reassing,
    sum(case when action = 'Unbundle order' then 1 else 0 end)                                              as n_unbundle,
    sum(case when action = 'Close' then 1 else 0 end)                                                       as n_close,
    sum(case when action = 'Cancel' then 1 else 0 end)                                                      as n_cancel,
    sum(case when action = 'Remake courier order' then 1 else 0 end)                                        as n_create_remake,   
    Count(distinct case when o.order_handling_strategy  <> 'PICKUP'
        and  o.order_final_status = 'DeliveredStatus' 
        and o.order_parent_relationship_type is null
        and action = 'Close' 
        and ((date_diff('second', 
            cast( o.order_courier_arrival_to_delivery_local_at as timestamp), 
            cast(o.order_terminated_local_at as timestamp))) <= 30
            or o.order_courier_arrival_to_delivery_local_at is null)
        and (date_diff('second', 
            cast( o.order_created_local_at as timestamp), 
            cast(o.order_terminated_local_at as timestamp))  / 60.00) between 55 and 62
            then o.order_id else null end)                                                                  as n_closed_before_arraiving

    FROM 
        delta.contact_audit_logs_odp.audit_log_entries al
    LEFT JOIN 
        delta.central_order_descriptors_odp.order_descriptors_v2 o
        on al.subject_id = cast(o.order_id as varchar)
    LEFT JOIN 
        delta.mfc_sales_odp.mfc_store_addresses_history  AS sa 
        ON o.store_address_id = sa.store_address_id
        AND sa.valid_to  >= CURRENT_DATE
    WHERE 
        creation_time >=  date('2025-01-01')
        AND subject = 'order'
        AND ACTOR_ID in (159711304, 159704216, 169967715, 129110901, 169987819, 169970834, 135248254, 167033812, 130375166, 134745627, 
        156667248, 156679576, 139733525, 140978495, 137911686, 139299680, 142064444, 133538197, 174746381, 169982513, 174745885, 170247631, 
        160688596, 142064454, 170157192, 169942666, 179587694, 168884214, 134060541, 168257296, 155097307, 157836095)
    group by 1,2,3,4,5,6,7,8
    order by 4 desc ,5,6
)

Select 

    cl.year,
    cl.month,
    cl.week,
    cl.date,
    --cl.hour,
    --cl.city,
    cl.operator,
    count (distinct mfc.warehouse_name) as n_shifts_mfcs,
    sum(cl.n_orders) as n_orders_by_cl,
    sum(mfc.total_orders) as mfc_total_orders,
    sum(mfc.orders_delivered) as mfc_order_delivered,
    sum(cl.n_assing) as n_assing,        
    sum(cl.n_reassing) as n_reassing,
    sum(cl.n_unbundle) as n_unbundle,
    sum(cl.n_close) as n_closed,
    sum(cl.n_closed_before_arraiving) as n_closed_before_arraiving,
    sum(cl.n_cancel) as n_canceled,
    sum(mfc.total_hard_cancellations) as mfc_hc,
    sum(mfc.total_night_cancellations) as night_hc,   
    sum(cl.n_create_remake) as n_created_remakes,
    sum(mfc.remakes) as mfc_remakes,
    sum(mfc.DT_more_60) as dt_more_60

From mfc
Right join cl
    on mfc.year = cl.year
    and mfc.month = cl.month
    and mfc.week = cl.week
    and mfc.date = cl.date
    and mfc.hour = cl.hour
    and mfc.city = cl.city
    and mfc.warehouse_name = cl.warehouse_name
Group by 1,2,3,4,5
Order by 1,2,3,4,7,5,6
