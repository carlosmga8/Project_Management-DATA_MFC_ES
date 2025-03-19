
    SELECT

        msa.country_code                                                                as country_code,
            case 
                when msa.city_code = 'BCN' then 'BCN' 
                when msa.city_code = 'MAD' then 'MAD' 
                else 'EXP' end                                                          as area_code,
        msa.city_code                                                                   as city,
        extract(year from o.order_activated_local_at)                                   as year,
        extract(month from o.order_activated_local_at)                                  as month,
        extract(week from o.order_activated_local_at)                                   as week,
        cast(date_trunc('day', o.order_activated_local_at) as date)                     as date,
        msa.warehouse_name                                                              as warehouse_name,
        count(distinct case
                when o.order_parent_relationship_type is null then o.order_id
                else null end)                                                          as placed_orders,
        count(distinct case
                when o.order_final_status = 'DeliveredStatus'
                    and o.order_parent_relationship_type is null then o.order_id
                else null end)                                                          as delivered_orders,
        count(distinct case
                when o.order_final_status = 'CanceledStatus'
                    and o.order_parent_id is null
                    and not ((date_diff('second', o.order_created_local_at,
                    o.order_terminated_local_at) / 60.0 <= 5 -- cancel_time_before_5min_of_creation
                        or (date_diff('second', o.order_activated_local_at,
                    o.order_terminated_local_at) / 60.0 <= 5 -- cancel_time_before_5min_of_activation
                    and o.order_scheduled_local_at is not null) -- order_is_scheduled
                        or (o.order_scheduled_local_at is not null
                    and o.order_activated_local_at is null)) -- order_is_scheduled_but_not_activated
                    and (o.order_cancel_reason is null
                        or o.order_cancel_reason in ('SELF_CANCELLATION','UNKNOWN','OTHER',
                    'DELIVERY_TAKING_TOO_LONG','COURIER_NOT_ASSIGNED',
                    'CUSTOMER_DOESNT_WANT_PRODUCTS'))) -- order_cancel_reason is null or in OPS reasons
                then o.order_id else null end)                                          as total_hard_cancellations,
        count(distinct case
                when o.order_final_status = 'DeliveredStatus'
                    and o.order_parent_relationship_type is null
                    and dta.delivery_time_in_seconds >= 60 * 60 then o.order_id
                else null end)                                                          as DT_more_60,
        count(distinct case
                when o.order_final_status = 'DeliveredStatus'
                    and o.order_parent_relationship_type is not null then o.order_id
                else null end) as remakes

    FROM
      delta.central_order_descriptors_odp.order_descriptors_v2 as o
      LEFT JOIN delta.mfc_sales_odp.mfc_store_addresses_history as msa on o.store_address_id = msa.store_address_id
      and order_activated_local_at BETWEEN msa.valid_from AND msa.valid_to
      LEFT JOIN delta.courier_order_flow_odp.delivery_times_order_level_attributes as dta on dta.order_id = o.order_id

    WHERE
        o.p_creation_date >= DATE(date_add('day', -120, current_date))
        and o.order_activated_local_at >= date_add('day', -120, current_date)
        and msa.warehouse_name is not null
        and msa.country_code = 'ES'
        and o.order_handling_strategy = 'GEN2'
    GROUP BY
      1,2,3,4,5,6,7,8
    ORDER BY
      1,2,3,4,5,6,7,8
 
