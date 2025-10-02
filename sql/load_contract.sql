INSERT INTO contract (contract_name, store_id)
SELECT DISTINCT ON (oc."КодТорговойТочки", oc."ДоговорКонтрагента")
       oc."ДоговорКонтрагента" AS contract_name,
       oc."КодТорговойТочки"::int AS store_id
FROM ods_contracts oc
WHERE oc."ДоговорКонтрагента" IS NOT NULL
  AND oc."КодТорговойТочки" IS NOT NULL
  AND oc.is_current = TRUE
ORDER BY oc."КодТорговойТочки", oc."ДоговорКонтрагента"
ON CONFLICT (contract_name, store_id) DO NOTHING;







