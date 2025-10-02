INSERT INTO transaction (
    product_id, store_id, supplier_id, manager_id,
    deal_type_id, delivery_type_id, order_id,
    transaction_type_id, date, cost_price,
    revenue_without_vat, revenue_with_vat, discount_amount,
    gross_profit, planned_gross_profit, quantity_sold,
    planned_quantity, planned_revenue, quantity_ordered, amount_ordered
)
SELECT DISTINCT ON (
        r."КодЗаказа", r."КодТовара", r."ТорговаяТочка", r."Дата"
    )
    p.product_id,
    s.store_id,
    sp.supplier_id,
    m.manager_id,
    dt.deal_type_id,
    d.delivery_type_id,
    o.order_id,
    tt.transaction_type_id,
    TO_DATE(r."Дата", 'DD.MM.YYYY'),

    -- Поля с возможными запятыми
    ROUND(
        REPLACE(r."Себестоимость"::text, ',', '.')::numeric
    , 4),
    ROUND(REPLACE(r."ВыручкабезНДС"::text, ',', '.')::numeric, 4),
    ROUND(REPLACE(r."ВыручкасНДС"::text, ',', '.')::numeric, 4),
    ROUND(REPLACE(r."СуммаСкидки"::text, ',', '.')::numeric, 4),
    ROUND(REPLACE(r."ВаловыйДоход"::text, ',', '.')::numeric, 4),
    ROUND(REPLACE(r."ПланВаловыйДоход"::text, ',', '.')::numeric, 4),
    ROUND(REPLACE(r."ОбъемПродажвКоличестве"::text, ',', '.')::numeric, 4),
    ROUND(REPLACE(r."НовыйПланКоличество"::text, ',', '.')::numeric, 4),
    ROUND(REPLACE(r."НовыйПланВыручка"::text, ',', '.')::numeric, 4),
    ROUND(REPLACE(r."ЗаказаноШт"::text, ',', '.')::numeric, 4),
    ROUND(REPLACE(r."ЗаказаноРуб"::text, ',', '.')::numeric, 4)
FROM ods_transactions r
JOIN product p ON p.product_code = r."КодТовара"
JOIN (SELECT DISTINCT store_name, store_id FROM store) s 
     ON s.store_name = r."ТорговаяТочка"
JOIN (SELECT DISTINCT supplier_name, supplier_id FROM supplier) sp
     ON sp.supplier_name = r."Поставщик"
JOIN (SELECT DISTINCT manager_name, manager_id FROM manager) m
     ON m.manager_name = r."Менеджер"
JOIN deal_type dt ON dt.deal_type_name = r."ТипСделки"
JOIN delivery_type d ON d.delivery_type_name = r."ТипДоставки"
JOIN orders o ON o.order_code = r."КодЗаказа"
JOIN transaction_type tt ON tt.transaction_name = r."ТипТранзакции"
WHERE r.is_current = TRUE
ORDER BY r."КодЗаказа", r."КодТовара", r."ТорговаяТочка", r."Дата", r.surrogate_key DESC

ON CONFLICT (order_id, product_id, store_id, date) DO NOTHING;
