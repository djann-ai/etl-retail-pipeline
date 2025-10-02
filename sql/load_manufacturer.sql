INSERT INTO manufacturer (manufacturer_name)
SELECT DISTINCT 
    r."Производитель" as manufacturer_name
FROM ods_products r
WHERE r.is_current = TRUE
ON CONFLICT (manufacturer_name)
DO NOTHING;