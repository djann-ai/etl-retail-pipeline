INSERT INTO store (store_id, store_name, address)
SELECT DISTINCT ON (r."КодТорговойТочки")
    r."КодТорговойТочки", 
    r."ТорговаяТочка",
    r."АдресДоставки"
FROM ods_stores r
WHERE r.is_current = TRUE
ORDER BY r."КодТорговойТочки", r.effective_from DESC
ON CONFLICT (store_id)
DO UPDATE SET 
    store_name = EXCLUDED.store_name,
    address = EXCLUDED.address;