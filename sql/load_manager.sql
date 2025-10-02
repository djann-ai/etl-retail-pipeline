INSERT INTO manager (manager_name, counterparty_id, branch_id)
SELECT DISTINCT r."Менеджер", c.counterparty_id, b.branch_id
FROM ods_counterparties r
JOIN counterparty c ON c.counterparty_id = r."КодКонтрагента"
JOIN branch b ON b.branch_name = r."Филиал"
WHERE r.is_current = TRUE
ON CONFLICT (manager_name, counterparty_id, branch_id)
DO NOTHING;
