CREATE TABLE IF NOT EXISTS shipment_drafts (
    job_ticket_number TEXT PRIMARY KEY,
    scanned_barcodes JSONB,
    updated_at TIMESTAMP DEFAULT NOW()
);
