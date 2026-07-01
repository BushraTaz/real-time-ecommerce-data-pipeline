CREATE TABLE IF NOT EXISTS dim_products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS fact_events (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    product_id INTEGER,
    event_type VARCHAR(50),
    event_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dlq_events (
    id SERIAL PRIMARY KEY,
    event JSONB,
    error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);