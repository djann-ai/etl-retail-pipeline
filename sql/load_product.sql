INSERT INTO product (product_code, product_name, line_id)
SELECT DISTINCT ON (r."КодТовара")
    r."КодТовара",
    r."Товар",
    pl.line_id
FROM ods_products r
JOIN brand b ON b.brand_code = r."КодБренда"
JOIN product_group pg ON pg.group_name = r."ГруппаТовара" AND pg.brand_id = b.brand_id
JOIN product_line pl ON pl.line_name = r."ЛинейкаТовара" AND pl.group_id = pg.group_id
WHERE r.is_current = TRUE
ORDER BY r."КодТовара", r."Товар"
ON CONFLICT (product_code)
DO UPDATE SET 
    product_name = EXCLUDED.product_name,
    line_id = EXCLUDED.line_id;
