-- Удаляем все таблицы, если они существуют
DROP TABLE IF EXISTS transaction CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS transaction_type CASCADE;
DROP TABLE IF EXISTS delivery_type CASCADE;
DROP TABLE IF EXISTS deal_type CASCADE;
DROP TABLE IF EXISTS manager CASCADE;
DROP TABLE IF EXISTS supplier CASCADE;
DROP TABLE IF EXISTS store CASCADE;
DROP TABLE IF EXISTS branch CASCADE;
DROP TABLE IF EXISTS counterparty CASCADE;
DROP TABLE IF EXISTS city CASCADE;
DROP TABLE IF EXISTS product CASCADE;
DROP TABLE IF EXISTS product_line CASCADE;
DROP TABLE IF EXISTS product_group CASCADE;
DROP TABLE IF EXISTS brand CASCADE;
DROP TABLE IF EXISTS manufacturer CASCADE;
DROP TABLE IF EXISTS contract CASCADE;

-- Справочники
CREATE TABLE manufacturer (
    manufacturer_id SERIAL PRIMARY KEY,
    manufacturer_name TEXT NOT NULL UNIQUE
);

CREATE TABLE brand (
    brand_id SERIAL PRIMARY KEY,
    brand_code TEXT UNIQUE NOT NULL,
    brand_name TEXT NOT NULL,
    manufacturer_id INT NOT NULL REFERENCES manufacturer(manufacturer_id)
);

CREATE TABLE product_group (
    group_id SERIAL PRIMARY KEY,
    group_name TEXT NOT NULL,
    brand_id INT NOT NULL REFERENCES brand(brand_id),
    UNIQUE (group_name, brand_id)
);

CREATE TABLE product_line (
    line_id SERIAL PRIMARY KEY,
    line_name TEXT NOT NULL,
    group_id INT NOT NULL REFERENCES product_group(group_id),
    UNIQUE (line_name, group_id)
);

CREATE TABLE product (
    product_id SERIAL PRIMARY KEY,
    product_code TEXT UNIQUE NOT NULL,
    product_name TEXT NOT NULL,
    line_id INT NOT NULL REFERENCES product_line(line_id)
);

CREATE TABLE city (
    city_id SERIAL PRIMARY KEY,
    city_name TEXT NOT NULL,
    region TEXT NOT NULL,
    UNIQUE(city_name, region)
);

CREATE TABLE counterparty (
    counterparty_id SERIAL PRIMARY KEY,
    counterparty_name TEXT NOT NULL,
    city_id INT REFERENCES city(city_id),
    latitude NUMERIC(18,6),
    longitude NUMERIC(18,6),
    full_address TEXT
);

CREATE TABLE store (
    store_id SERIAL PRIMARY KEY,
    store_name TEXT NOT NULL,
    address TEXT
);

CREATE TABLE contract (
    contract_id SERIAL PRIMARY KEY,
    contract_name TEXT NOT NULL,
    store_id INT NOT NULL REFERENCES store(store_id),
    CONSTRAINT uq_contract_store UNIQUE (contract_name, store_id)
);

CREATE TABLE supplier (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name TEXT NOT NULL UNIQUE
);

CREATE TABLE branch (
    branch_id SERIAL PRIMARY KEY,
    branch_name TEXT UNIQUE NOT NULL
);

CREATE TABLE manager (
    manager_id SERIAL PRIMARY KEY,
    manager_name TEXT NOT NULL,
    counterparty_id INT NOT NULL REFERENCES counterparty(counterparty_id),
    branch_id INT NOT NULL REFERENCES branch(branch_id),
    UNIQUE (manager_name, counterparty_id, branch_id)
);

CREATE TABLE deal_type (
    deal_type_id SERIAL PRIMARY KEY,
    deal_type_name TEXT UNIQUE NOT NULL
);

CREATE TABLE delivery_type (
    delivery_type_id SERIAL PRIMARY KEY,
    delivery_type_name TEXT UNIQUE NOT NULL
);

CREATE TABLE transaction_type (
    transaction_type_id SERIAL PRIMARY KEY,
    transaction_name TEXT UNIQUE NOT NULL
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    order_code INT NOT NULL,
    order_number INT NOT NULL
);

-- Основная таблица транзакций
CREATE TABLE transaction (
    transaction_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES product(product_id),
    store_id INT NOT NULL REFERENCES store(store_id),
    supplier_id INT NOT NULL REFERENCES supplier(supplier_id),
    manager_id INT NOT NULL REFERENCES manager(manager_id),
    deal_type_id INT NOT NULL REFERENCES deal_type(deal_type_id),
    delivery_type_id INT NOT NULL REFERENCES delivery_type(delivery_type_id),
    order_id INT NOT NULL REFERENCES orders(order_id),
    transaction_type_id INT NOT NULL REFERENCES transaction_type(transaction_type_id),
    date DATE NOT NULL,
    cost_price NUMERIC(18,4),
    revenue_without_vat NUMERIC(18,4),
    revenue_with_vat NUMERIC(18,4),
    discount_amount NUMERIC(18,4),
    gross_profit NUMERIC(18,4),
    planned_gross_profit NUMERIC(18,4),
    quantity_sold NUMERIC(18,4),
    planned_quantity NUMERIC(18,4),
    planned_revenue NUMERIC(18,4),
    quantity_ordered NUMERIC(18,4),
    amount_ordered NUMERIC(18,4)
);

-- Индексы для ускорения поиска
CREATE INDEX idx_contract_store_id ON contract(store_id);
CREATE INDEX idx_counterparty_city_id ON counterparty(city_id);
CREATE INDEX idx_manager_counterparty_id ON manager(counterparty_id);
CREATE INDEX idx_manager_branch_id ON manager(branch_id);
CREATE INDEX idx_transaction_product_id ON transaction(product_id);
CREATE INDEX idx_transaction_store_id ON transaction(store_id);
CREATE INDEX idx_transaction_supplier_id ON transaction(supplier_id);
CREATE INDEX idx_transaction_manager_id ON transaction(manager_id);
CREATE INDEX idx_transaction_deal_type_id ON transaction(deal_type_id);
CREATE INDEX idx_transaction_delivery_type_id ON transaction(delivery_type_id);
CREATE INDEX idx_transaction_order_id ON transaction(order_id);
CREATE INDEX idx_transaction_transaction_type_id ON transaction(transaction_type_id);
CREATE INDEX idx_transaction_date ON transaction(date);
CREATE INDEX idx_product_product_code ON product(product_code);
CREATE INDEX idx_brand_brand_code ON brand(brand_code);
CREATE INDEX idx_contract_contract_name ON contract(contract_name);
CREATE INDEX idx_store_store_name ON store(store_name);
CREATE INDEX idx_counterparty_counterparty_name ON counterparty(counterparty_name);
CREATE INDEX idx_manager_manager_name ON manager(manager_name);
CREATE INDEX idx_product_line_group_id ON product_line(group_id);
CREATE INDEX idx_product_group_brand_id ON product_group(brand_id);
CREATE INDEX idx_brand_manufacturer_id ON brand(manufacturer_id);

-- Уникальный индекс для предотвращения дублирования транзакций
CREATE UNIQUE INDEX transaction_unique_idx
ON transaction (order_id, product_id, store_id, date);
