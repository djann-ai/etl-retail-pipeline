-- STG таблица для Товаров
CREATE TABLE IF NOT EXISTS stg_products (
    "Бренд" TEXT,
    "ГруппаТовара" TEXT,
    "КодБренда" TEXT,
    "КодТовара" TEXT,
    "ЛинейкаТовара" TEXT,
    "Производитель" TEXT,
    "Товар" TEXT
);

-- STG таблица для Транзакций
CREATE TABLE IF NOT EXISTS stg_transactions (
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
    "Поставщик" TEXT
);

-- STG таблица для Торговых Точек
CREATE TABLE IF NOT EXISTS stg_stores (
    "АдресДоставки" TEXT,
    "ДоговорКонтрагента" TEXT,
    "КодТорговойТочки" INTEGER,
    "ТорговаяТочка" TEXT
);

-- STG таблица для Контрагентов
CREATE TABLE IF NOT EXISTS stg_counterparties (
    "City" TEXT,
    "АдресТТ" TEXT,
    "Город" TEXT,
    "Долгота" TEXT,
    "КодКонтрагента" INTEGER,
    "Контрагент" TEXT,
    "Менеджер" TEXT,
    "Область" TEXT,
    "Филиал" TEXT,
    "Широта" TEXT
);

-- STG таблица для Договоров
CREATE TABLE IF NOT EXISTS stg_contracts (
    "КодТорговойТочки" INTEGER,
    "ДоговорКонтрагента" TEXT
);