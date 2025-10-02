INSERT INTO product_line (line_name, group_id)
SELECT DISTINCT r."ЛинейкаТовара", pg.group_id
FROM ods_products r
JOIN product_group pg ON pg.group_name = r."ГруппаТовара"
WHERE r.is_current = TRUE
ON CONFLICT (line_name, group_id)
DO NOTHING;