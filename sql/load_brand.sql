INSERT INTO brand (brand_code, brand_name, manufacturer_id)
SELECT DISTINCT ON (r."КодБренда")
    r."КодБренда",
    r."Бренд",
    m.manufacturer_id
FROM ods_products r
JOIN manufacturer m ON m.manufacturer_name = r."Производитель"
WHERE r.is_current = TRUE
ORDER BY r."КодБренда", r."Бренд"
ON CONFLICT (brand_code)
DO UPDATE SET 
    brand_name = EXCLUDED.brand_name,
    manufacturer_id = EXCLUDED.manufacturer_id;