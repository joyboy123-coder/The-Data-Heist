SELECT 
*
FROM {{ref('clean_orders')}}
WHERE STATUS = 'Delivered'