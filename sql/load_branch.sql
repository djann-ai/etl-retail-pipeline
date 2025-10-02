INSERT INTO branch (branch_name)
SELECT DISTINCT "Филиал"
FROM ods_counterparties 
WHERE "Филиал" IS NOT NULL 
AND "Филиал" != ''
ON CONFLICT (branch_name) DO NOTHING;