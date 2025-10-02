INSERT INTO counterparty (counterparty_id, counterparty_name, city_id, latitude, longitude, full_address)
SELECT DISTINCT ON ("КодКонтрагента")
    "КодКонтрагента",
    "Контрагент",
    c.city_id,
    NULLIF(REPLACE("Широта", ',', '.'), '')::NUMERIC,
    NULLIF(REPLACE("Долгота", ',', '.'), '')::NUMERIC,
    "АдресТТ"
FROM ods_counterparties r
LEFT JOIN city c ON c.city_name = r."Город" AND c.region = r."Область"
WHERE "КодКонтрагента" IS NOT NULL
  AND "Контрагент" IS NOT NULL
  AND r.is_current = TRUE
ORDER BY "КодКонтрагента", effective_from DESC
ON CONFLICT (counterparty_id) DO UPDATE SET
    counterparty_name = EXCLUDED.counterparty_name,
    city_id = EXCLUDED.city_id,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    full_address = EXCLUDED.full_address;