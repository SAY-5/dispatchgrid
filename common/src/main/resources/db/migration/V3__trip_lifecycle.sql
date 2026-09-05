ALTER TABLE trips
  ADD COLUMN completed_at TIMESTAMP(3) NULL AFTER matched_at,
  ADD COLUMN cancelled_at TIMESTAMP(3) NULL AFTER completed_at;
