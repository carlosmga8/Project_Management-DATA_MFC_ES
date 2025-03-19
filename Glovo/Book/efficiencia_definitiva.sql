WITH orders as (
            SELECT o.order_id,
            o.order_activated_local_at,
            o.order_parent_relationship_type,
            o.order_final_status,o.order_parent_id,
            o.order_created_local_at,
            o.order_terminated_local_at,
            o.order_scheduled_local_at,
            o.order_cancel_reason,
            o.order_picked_up_by_courier_at,
            o.order_accepted_by_partner_at,
            o.order_courier_arrival_to_pickup_at,
            o.store_address_id,
            o.order_handling_strategy,
            CASE WHEN abs(date_diff('MINUTE',
            o.order_picked_up_by_courier_at,
            lead(o.order_picked_up_by_courier_at) OVER (PARTITION BY courier_id ORDER BY o.order_picked_up_by_courier_at)
                )) <= 5
                OR abs(date_diff('MINUTE',
            o.order_picked_up_by_courier_at,
            lag(o.order_picked_up_by_courier_at) OVER (PARTITION BY courier_id ORDER BY o.order_picked_up_by_courier_at)
                )) <= 5
            THEN TRUE ELSE FALSE END as is_true_bundle
    FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
    LEFT JOIN   delta.mfc_sales_odp.mfc_store_addresses_history as msa on o.store_address_id = msa.store_address_id
    and o.order_activated_local_at BETWEEN msa.valid_from AND msa.valid_to
    WHERE o.p_creation_date >= DATE (date_add('day', -900, current_date))
            and o.order_activated_local_at >= date_add('day', -900, current_date)
            and o.order_handling_strategy  = 'GEN2'
            and msa.country_code = 'ES'
),
f as (
SELECT      coalesce( msa.city_code , substring( msa.warehouse_name,1,3) )    as city,
            --msa.warehouse_name                                                  as mfc,
            extract(year from o.order_activated_local_at)                       as year,
            --extract(month from o.order_activated_local_at)                      as month,
            count( distinct case when o.order_parent_relationship_type is null
                                        then    o.order_id
                                        else    null   end)                     as placed_orders,

            count( distinct  case  when   o.order_final_status = 'DeliveredStatus' and o.order_parent_relationship_type is null
                                        then    o.order_id
                                        else    null    end)                    as orders_delivered,


            count( distinct  case  when   o.order_final_status = 'DeliveredStatus' and o.order_parent_relationship_type is not null
                                   then    o.order_id
                                   else    null    end)                         as remakes

FROM        orders                          as o

LEFT JOIN   delta.mfc_sales_odp.mfc_store_addresses_history                                 as msa  on o.store_address_id = msa.store_address_id
and order_activated_local_at BETWEEN msa.valid_from AND msa.valid_to
LEFT JOIN   delta.courier_order_flow_odp.delivery_times_order_level_attributes              as dta  on dta.order_id = o.order_id
LEFT JOIN   delta.courier_order_flow_odp.delivery_times_logistics_order_level_attributes    as dta2  on dta2.order_id = o.order_id
LEFT JOIN   delta.courier_flow_odp.courier_flow                                             as cou  on cou.order_id = o.order_id
LEFT JOIN   delta.courier_order_flow_odp.courier_distances_order_level_attributes           as dis  on dis.order_id = o.order_id
LEFT JOIN   delta.courier_order_management_odp.bundled_order_details                        as bund on bund.order_id = o.order_id
LEFT JOIN   delta.courier_delivery_flow_odp.order_courier_assignments                       as ass  on ass.order_id = o.order_id
LEFT JOIN   delta.courier_order_flow_odp.delivery_times_logistics_order_level_attributes    as stk  on stk.order_id = o.order_id
LEFT JOIN   delta.central_dispatched_partner_orders_odp.dispatched_partner_orders           as dpo  on dpo.order_id = o.order_id
LEFT JOIN   delta.courier_partner_quality_odp.dispatching_partner_times                     as disp on  disp.order_id = o.order_id
LEFT JOIN   delta.courier_partner_quality_odp.order_ready                                   as orb  on  o.order_id = orb.order_id            

WHERE       msa.warehouse_name is not null and msa.country_code = 'ES'
            and msa.city_code in('BCN','MAD')

GROUP BY    1,2
ORDER BY    1,2,3,4,5)
select * from f order by 1,2

----Query couriers 12-11 ----
WITH
  ranges as (
    select
      date_add('day', -900, current_date) as rango_fecha
  ),
  slots as (
    select
      slo.courier_id,
      slo.store_address_id,
      extract(
        year
        from
          slo.slot_started_local_at
      ) as year,
      extract(
        month
        from
          date_trunc('day', slo.slot_started_local_at)
      ) as month,
      extract(
        week
        from
          date_trunc('day', slo.slot_started_local_at)
      ) as week_of_the_year,
      cast(
        date_trunc('day', slo.slot_started_local_at) as date
      ) as date_day,
      sum(slot_duration_time_in_minutes) / 60.0 as booked_hours,
      sum(
        case
          when v.vehicle in ('MOTO 125', 'MOTO 50', 'MOTO') then slot_duration_time_in_minutes
          else 0
        end
      ) / 60.0 as booked_hours_moto,
      sum(
        case
          when no_show_time_in_minutes = 30 then no_show_time_in_minutes
          else 0
        end
      ) / 60.0 as no_show_hours,
      sum(
        case
          when slot_start_to_check_in_time_in_minutes = 30 then 0
          else slot_start_to_check_in_time_in_minutes
        end
      ) / 60.0 as late_check_ins,
      sum(
        order_courier_delivery_time_within_slot_in_minutes
      ) / 60.0 as busy_time
    from
      delta.courier_performance_odp.courier_performance_courier_slot_level_components slo
      inner join "delta"."mfc_sales_odp"."mfc_store_addresses_history" msa on msa.store_address_id = slo.store_address_id
      and country_code = 'ES'
      AND slo.slot_started_local_at between valid_from AND valid_to
      left join delta.courier__in_house_supply__odp.spreadsheet_in_house_fleet_vehicle_history as v on v.courier_id = slo.courier_id
      and slo.slot_started_local_at >= v.start_date
      and case
        when v.end_date is null then slo.slot_started_local_at <= current_date
        else slo.slot_started_local_at <= v.end_date
      end
    where
      slo.p_slot_started_date >= (
        select
          rango_fecha
        from
          ranges
      )
      and slo.is_booked_to_start_slot = TRUE
    group by
      1,
      2,
      3,
      4,
      5,
      6
  ),
  idle as (
    select
      courier_id,
      store_address_id,
      year,
      month,
      week_of_the_year,
      date_day,
      booked_hours,
      booked_hours_moto,
      no_show_hours,
      late_check_ins,
      busy_time,
      case
        when (
          booked_hours - no_show_hours - late_check_ins - busy_time
        ) < 0 then 0
        else (
          booked_hours - no_show_hours - late_check_ins - busy_time
        )
      end as idle_time
    from
      slots
  ),
  orders_base as (
    select
      slo.courier_id,
      extract(
        year
        from
          slo.slot_started_local_at
      ) as year,
      extract(
        month
        from
          date_trunc('day', slo.slot_started_local_at)
      ) as month,
      extract(
        week
        from
          date_trunc('day', slo.slot_started_local_at)
      ) as week_of_the_year,
      cast(
        date_trunc('day', slo.slot_started_local_at) as date
      ) as date_day,
      slo.store_address_id as store_address_id,
      case
        when slo.no_show_time_in_minutes = 30 then 0
        else 1
      end as courier_worked,
      count(
        distinct case
          when o.order_final_status = 'DeliveredStatus'
          and cf.order_accepted_by_courier_at_local between slo.slot_started_local_at and slo.slot_finished_local_at  then o.order_id
          else null
        end
      ) as delivered_orders
    from
      delta.courier_performance_odp.courier_performance_courier_slot_level_components slo
      inner join "delta"."mfc_sales_odp"."mfc_store_addresses_history" msa on msa.store_address_id = slo.store_address_id
      and country_code = 'ES'
      left join delta.central_order_descriptors_odp.order_descriptors_v2 o on o.courier_id = slo.courier_id
      and o.p_creation_date >= (
        select
          rango_fecha
        from
          ranges
      )
      left join delta.courier_flow_odp.courier_flow cf on cf.order_id = o.order_id
    where
      slo.p_slot_started_date >= (
        select
          rango_fecha
        from
          ranges
      )
      and slo.is_booked_to_start_slot = TRUE
    group by
      1,
      2,
      3,
      4,
      5,
      6,
      7
  ),
  orders as (
    select
      courier_id,
      store_address_id,
      year,
      month,
      week_of_the_year,
      date_day,
      sum(
        case
          when courier_worked = 1
          and delivered_orders = 0 then 1
          else 0
        end
      ) as courier_without_orders
    from
      orders_base
    group by
      1,
      2,
      3,
      4,
      5,
      6
  ),
  
pre as (
    select
      s.courier_id,
      s.store_address_id,
      s.year,
      s.month,
      s.week_of_the_year,
      s.date_day,
      o.courier_without_orders,
       msa.city_code,
      msa.warehouse_name as mfc_name,
      sum(s.booked_hours) booked_hours,
      sum(s.booked_hours_moto) booked_hours_moto,
      sum(s.late_check_ins) late_check_ins,
      sum(s.no_show_hours) no_show_hours,
      sum(s.busy_time) busy_time,
      sum(s.idle_time) idle_time
    from
      idle s
      left join orders o on o.date_day = s.date_day
      and o.courier_id = s.courier_id
      and o.month = s.month
      and o.store_address_id = s.store_address_id
      left join "delta"."mfc_sales_odp"."mfc_store_addresses_history" msa on msa.store_address_id = s.store_address_id
      and country_code = 'ES'
      AND s.date_day between valid_from AND valid_to
      ----aquí quitamos los couriers de CES
    where
      s.courier_id not in (
        145430714,
        145430716,
        145430717,
        145509138,
        152403765,
        84594026
      )
    group by
      1,
      2,
      3,
      4,
      5,
      6,
      7,8,9
  )
    select
      s.city_code,
    --mfc_name,
    s.year,
      --s.month,
  
      case
        when  mfc_name = 'Fake Store Supply/Ops MFCs' then 'fakeStore'
        else null
      end as fake_store,
      sum(s.booked_hours) booked_hours,
      sum(s.booked_hours_moto) booked_hours_moto,
      sum(s.late_check_ins) late_check_ins,
      sum(s.no_show_hours) no_show_hours,
      sum(s.busy_time) busy_time,
      sum(s.idle_time) idle_time,
      sum(s.courier_without_orders) courier_without_orders,
      sum(s.booked_hours) - (sum(s.no_show_hours) +  (sum(s.late_check_ins) /60.00)) as checked_hour
    from
      pre s
    where mfc_name  <> 'Fake Store Supply/Ops MFCs'
    and      s.city_code in ('MAD','BCN')
          --and UPPER(msa.warehouse_name) like '%FAKE%'
    group by
      1,2,3
