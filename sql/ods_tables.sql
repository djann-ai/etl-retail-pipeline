-- Таблица: ods_products
CREATE TABLE IF NOT EXISTS ods_products (
    surrogate_key SERIAL PRIMARY KEY,
    "Бренд" TEXT,
    "ГруппаТовара" TEXT,
    "КодБренда" TEXT,
    "КодТовара" TEXT,
    "ЛинейкаТовара" TEXT,
    "Производитель" TEXT,
    "Товар" TEXT,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_ods_products_current ON ods_products(is_current);
CREATE INDEX IF NOT EXISTS idx_ods_products_code ON ods_products("КодТовара");
CREATE INDEX IF NOT EXISTS idx_ods_products_effective ON ods_products(effective_from, effective_to);

COMMENT ON TABLE ods_products IS 'Товары с историчностью SCD2';
COMMENT ON COLUMN ods_products.effective_from IS 'Начало действия версии записи';
COMMENT ON COLUMN ods_products.effective_to IS 'Конец действия версии записи';
COMMENT ON COLUMN ods_products.is_current IS 'Флаг актуальности записи (TRUE - актуальная)';

-- Таблица: ods_transactions
CREATE TABLE IF NOT EXISTS ods_transactions (
    surrogate_key SERIAL PRIMARY KEY,
    "КодПоставщика" TEXT,
    "%ГодМесяц" TEXT,
    "№строки" INTEGER,
    "Дата" TEXT,
    "ТорговаяТочка" TEXT,
    "КодТовара" TEXT,
    "Менеджер" TEXT,
    "ДокументРеализации" TEXT,
    "№заказа" INTEGER,
    "КодЗаказа" INTEGER,
    "ТипТранзакции" TEXT,
    "ВыручкабезНДС" TEXT,
    "НовыйПланВыручка" TEXT,
    "ОбъемПродажвКоличестве" INTEGER,
    "НовыйПланКоличество" TEXT,
    "Себестоимость" TEXT,
    "ДокументЗаказа" TEXT,
    "НДС" TEXT,
    "ВыручкасНДС" TEXT,
    "СуммаСкидки" TEXT,
    "ВаловыйДоход" TEXT,
    "ПланВаловыйДоход" TEXT,
    "ЗаказаноШт" INTEGER,
    "ЗаказаноРуб" TEXT,
    "ТипСделки" TEXT,
    "ТипДоставки" TEXT,
    "Поставщик" TEXT,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_ods_transactions_current ON ods_transactions(is_current);
CREATE INDEX IF NOT EXISTS idx_ods_transactions_date ON ods_transactions("Дата");
CREATE INDEX IF NOT EXISTS idx_ods_transactions_effective ON ods_transactions(effective_from, effective_to);

COMMENT ON TABLE ods_transactions IS 'Транзакции с историчностью SCD2';

-- Таблица: ods_stores
CREATE TABLE IF NOT EXISTS ods_stores (
    surrogate_key SERIAL PRIMARY KEY,
    "АдресДоставки" TEXT,
    "ДоговорКонтрагента" TEXT,
    "КодТорговойТочки" INTEGER,
    "ТорговаяТочка" TEXT,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_ods_stores_current ON ods_stores(is_current);
CREATE INDEX IF NOT EXISTS idx_ods_stores_code ON ods_stores("КодТорговойТочки");
CREATE INDEX IF NOT EXISTS idx_ods_stores_effective ON ods_stores(effective_from, effective_to);

COMMENT ON TABLE ods_stores IS 'Торговые точки с историчностью SCD2';

-- Таблица: ods_counterparties
CREATE TABLE IF NOT EXISTS ods_counterparties (
    surrogate_key SERIAL PRIMARY KEY,
    "City" TEXT,
    "АдресТТ" TEXT,
    "Город" TEXT,
    "Долгота" TEXT,
    "КодКонтрагента" INTEGER,
    "Контрагент" TEXT,
    "Менеджер" TEXT,
    "Область" TEXT,
    "Филиал" TEXT,
    "Широта" TEXT,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_ods_counterparties_current ON ods_counterparties(is_current);
CREATE INDEX IF NOT EXISTS idx_ods_counterparties_code ON ods_counterparties("КодКонтрагента");
CREATE INDEX IF NOT EXISTS idx_ods_counterparties_effective ON ods_counterparties(effective_from, effective_to);

COMMENT ON TABLE ods_counterparties IS 'Контрагенты с историчностью SCD2';

-- Таблица: ods_contracts
CREATE TABLE IF NOT EXISTS ods_contracts (
    surrogate_key SERIAL PRIMARY KEY,
    "КодТорговойТочки" INTEGER,
    "ДоговорКонтрагента" TEXT,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_ods_contracts_current ON ods_contracts(is_current);
CREATE INDEX IF NOT EXISTS idx_ods_contracts_store_code ON ods_contracts("КодТорговойТочки");
CREATE INDEX IF NOT EXISTS idx_ods_contracts_contract ON ods_contracts("ДоговорКонтрагента");
CREATE INDEX IF NOT EXISTS idx_ods_contracts_effective ON ods_contracts(effective_from, effective_to);

COMMENT ON TABLE ods_contracts IS 'Договоры с историчностью SCD2';
