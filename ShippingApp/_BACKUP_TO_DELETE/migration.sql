-- migration.sql

-- 1. Modify item_boxes table
ALTER TABLE item_boxes ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'generated';
ALTER TABLE item_boxes ADD COLUMN IF NOT EXISTS packed_at TIMESTAMP;

-- 2. Create shipping_cartons table
CREATE TABLE IF NOT EXISTS shipping_cartons (
    code TEXT PRIMARY KEY,
    name TEXT,
    weight FLOAT,
    length FLOAT,
    width FLOAT,
    height FLOAT
);

-- Seed Cartons
INSERT INTO shipping_cartons (code, name, weight, length, width, height) VALUES
('#105', 'Box #105', 0.4, 6.0, 6.0, 4.0),
('#115', 'Box #115', 0.6, 9.0, 6.0, 4.0),
('#116', 'Box #116', 0.8, 10.0, 10.0, 6.0),
('#160', 'Box #160', 1.0, 12.0, 12.0, 8.0),
('#145', 'Box #145', 1.2, 14.0, 14.0, 10.0)
ON CONFLICT (code) DO UPDATE SET
    weight = EXCLUDED.weight,
    length = EXCLUDED.length,
    width = EXCLUDED.width,
    height = EXCLUDED.height;

-- 3. Create product_shipping_rules table
CREATE TABLE IF NOT EXISTS product_shipping_rules (
    id SERIAL PRIMARY KEY,
    category_name TEXT,
    quantity INTEGER,
    box_weight FLOAT,
    UNIQUE(category_name, quantity)
);

-- Seed initial rules (Placeholders)
INSERT INTO product_shipping_rules (category_name, quantity, box_weight) VALUES
('12ptBounceBack', 250, 1.5),
('12ptBounceBack', 500, 3.0),
('12ptBounceBack', 1000, 6.0),
('16ptBusinessCard', 250, 0.5),
('16ptBusinessCard', 500, 1.0)
ON CONFLICT (category_name, quantity) DO NOTHING;
