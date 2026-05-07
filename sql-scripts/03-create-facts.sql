CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    seller_id INTEGER,
    product_id INTEGER,
    store_id INTEGER,
    supplier_id INTEGER,
    date_id INTEGER,
    quantity INTEGER,
    total_price DECIMAL(10,2),
    unit_price DECIMAL(10,2)
);

CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_seller ON fact_sales(seller_id);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_store ON fact_sales(store_id);
CREATE INDEX idx_fact_sales_supplier ON fact_sales(supplier_id);
CREATE INDEX idx_fact_sales_date ON fact_sales(date_id);
