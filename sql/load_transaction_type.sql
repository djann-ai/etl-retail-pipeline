INSERT INTO transaction_type (transaction_name)
SELECT DISTINCT r."ТипТранзакции"
FROM ods_transactions r
WHERE r.is_current = TRUE
ON CONFLICT (transaction_name) DO NOTHING;
