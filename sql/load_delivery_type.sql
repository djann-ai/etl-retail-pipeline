INSERT INTO delivery_type (delivery_type_name)
SELECT DISTINCT r."ТипДоставки"
FROM ods_transactions r
WHERE r.is_current = TRUE
ON CONFLICT (delivery_type_name) DO NOTHING;
