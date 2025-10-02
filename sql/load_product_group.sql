INSERT INTO product_group (group_name, brand_id)
SELECT DISTINCT r."ГруппаТовара", b.brand_id
FROM ods_products r
JOIN brand b ON b.brand_name = r."Бренд"
WHERE r.is_current = TRUE
ON CONFLICT (group_name, brand_id)
DO NOTHING;