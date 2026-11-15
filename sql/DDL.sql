CREATE DATABASE fx_rates_db;

CREATE TABLE fx_rates (
    id SERIAL PRIMARY KEY,
    provider VARCHAR(50),
    base_currency VARCHAR(10),
    target_currency VARCHAR(10),
    exchange_rate NUMERIC(18,8),
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE pipeline_audit (
    id SERIAL PRIMARY KEY,
    process_name VARCHAR(100),
    status VARCHAR(50),
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
