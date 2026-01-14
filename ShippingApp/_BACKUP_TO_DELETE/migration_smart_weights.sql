-- migration_smart_weights.sql

-- 1. Create product_categories table
-- Maps a specific Product ID (e.g. "180", "12pt-BB-100") to a Category Name (e.g. "Outsource", "12ptBounceBack")
CREATE TABLE IF NOT EXISTS product_categories (
    product_id TEXT PRIMARY KEY,
    category_name TEXT NOT NULL
);

-- 2. Modify product_shipping_rules table
-- Maps Category + Quantity -> Weight PER BOX and Number of ITEM BOXES
-- We need to add num_item_boxes if it's not there, or maybe 'boxes_required'
-- The user said: "find the number of item boxes required. find the weight of these item boxes."

-- Check if column exists first (idempotent)
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='product_shipping_rules' AND column_name='num_boxes') THEN
        ALTER TABLE product_shipping_rules ADD COLUMN num_boxes INTEGER DEFAULT 1;
    END IF;
END $$;
