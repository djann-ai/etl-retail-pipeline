INSERT INTO supplier (supplier_name)
SELECT DISTINCT 
    r."Поставщик" as supplier_name
FROM ods_transactions r
WHERE r.is_current = TRUE
ON CONFLICT (supplier_name)
DO UPDATE SET 
    supplier_name = EXCLUDED.supplier_name;