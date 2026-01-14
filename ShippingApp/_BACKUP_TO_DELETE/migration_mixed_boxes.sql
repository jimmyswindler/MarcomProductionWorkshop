-- migration_mixed_boxes.sql

-- Modify product_shipping_rules to support Mixed Boxes (White vs Blue)
-- We will drop the old columns (box_weight, num_boxes) and add new ones if they don't exist.
-- To be safe, we can just add the new ones and ignore the old ones for now.

ALTER TABLE product_shipping_rules ADD COLUMN IF NOT EXISTS white_box_weight FLOAT;
ALTER TABLE product_shipping_rules ADD COLUMN IF NOT EXISTS blue_box_weight FLOAT;
ALTER TABLE product_shipping_rules ADD COLUMN IF NOT EXISTS white_box_qty INTEGER DEFAULT 0;
ALTER TABLE product_shipping_rules ADD COLUMN IF NOT EXISTS blue_box_qty INTEGER DEFAULT 0;

-- Optional: Drop old columns if we are sure
-- ALTER TABLE product_shipping_rules DROP COLUMN IF EXISTS box_weight;
-- ALTER TABLE product_shipping_rules DROP COLUMN IF EXISTS num_boxes;
