INSERT INTO deal_type (deal_type_name)
SELECT DISTINCT r."ТипСделки"
FROM ods_transactions r
WHERE r.is_current = TRUE
ON CONFLICT (deal_type_name) DO NOTHING;
