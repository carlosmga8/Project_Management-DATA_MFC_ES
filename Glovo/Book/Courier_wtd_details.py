SELECT 
  mfc_store_addresses.warehouse_name AS "mfc_store_addresses.mfc_name",
  DATE_FORMAT(order_descriptors.order_created_local_at, '%Y-%m-%d %H:%i:%s') AS "order_descriptors.order_created_local_at",
  order_descriptors.order_id,
  courier_id,
  DATE_FORMAT(order_courier_arrival_to_delivery_local_at, '%H:%i:%s') AS "Arrival to delivery",
  DATE_FORMAT(order_terminated_local_at, '%H:%i:%s') AS "Order Terminated",
  ROUND((DATE_DIFF('second', CAST(order_courier_arrival_to_delivery_local_at AS TIMESTAMP), CAST(order_terminated_local_at AS TIMESTAMP)) / 60.0), 2) AS WTD,
  delivery_issue_type
FROM 
  delta.central_order_descriptors_odp.order_descriptors_v2 AS order_descriptors
LEFT JOIN delta.mfc_sales_odp.mfc_store_addresses_history AS mfc_store_addresses ON order_descriptors.store_address_id = mfc_store_addresses.store_address_id
LEFT JOIN delta.contact__delivery_issues__odp.fct_delivery_issues_automation AS delivery_issue ON order_descriptors.order_id = delivery_issue.order_id
WHERE
  (order_descriptors.courier_id) = 74712104
  AND (((order_descriptors.order_created_local_at) >= TIMESTAMP '2024-04-19' AND (order_descriptors.order_created_local_at) < DATE_ADD('day', 1, TIMESTAMP '2024-04-30'))) 
  AND ((order_descriptors.order_created_local_at) >= DATE_ADD('day', -364, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)) AND (order_descriptors.order_created_local_at) < DATE_ADD('day', 365, DATE_ADD('day', -364, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))
  AND (mfc_store_addresses.warehouse_name IS NOT NULL OR mfc_store_addresses.warehouse_business_id IS NOT NULL )
  AND (order_descriptors.order_country_code) = 'ES'
  AND ROUND((DATE_DIFF('second', CAST(order_courier_arrival_to_delivery_local_at AS TIMESTAMP), CAST(order_terminated_local_at AS TIMESTAMP)) / 60.0), 2) > 5.0
ORDER BY 2 ASC 
