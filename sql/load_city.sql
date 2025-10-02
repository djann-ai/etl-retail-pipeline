INSERT INTO city (city_name, region)
SELECT DISTINCT 
    r."Город" as city_name, 
    r."Область" as region
FROM ods_counterparties r
WHERE r.is_current = TRUE
ON CONFLICT (city_name, region)
DO NOTHING;