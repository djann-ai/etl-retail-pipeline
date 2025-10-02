INSERT INTO orders (order_code, order_number)
SELECT DISTINCT 
    "КодЗаказа",
    "№заказа"
FROM ods_transactions 
WHERE is_current = TRUE;