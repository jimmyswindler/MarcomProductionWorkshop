
-- Indexes for Dashboard Performance

-- Orders: Date filtering and joining
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_ship_date ON orders(ship_date);
CREATE INDEX IF NOT EXISTS idx_orders_actual_ship_date ON orders(actual_ship_date);

-- Shipments: Date filtering
CREATE INDEX IF NOT EXISTS idx_shipments_ship_date ON shipments(ship_date);

-- Items/Jobs: Foreign keys (already checked, but ensuring)
CREATE INDEX IF NOT EXISTS idx_items_job_id ON items(job_id);
CREATE INDEX IF NOT EXISTS idx_jobs_order_id ON jobs(order_id);
