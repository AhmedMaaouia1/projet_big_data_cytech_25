-- =========================
-- FACT TABLE INDEXES
-- =========================

CREATE INDEX IF NOT EXISTS idx_fact_trip_pickup_date
ON fact_trip (pickup_date);

CREATE INDEX IF NOT EXISTS idx_fact_trip_pickup_time
ON fact_trip (pickup_time);

CREATE INDEX IF NOT EXISTS idx_fact_trip_pickup_location
ON fact_trip (pickup_location_id);

CREATE INDEX IF NOT EXISTS idx_fact_trip_dropoff_location
ON fact_trip (dropoff_location_id);

CREATE INDEX IF NOT EXISTS idx_fact_trip_vendor
ON fact_trip (vendor_id);

CREATE INDEX IF NOT EXISTS idx_fact_trip_payment_type
ON fact_trip (payment_type_id);

CREATE INDEX IF NOT EXISTS idx_fact_trip_ratecode
ON fact_trip (ratecode_id);

-- =========================
-- FACT TABLE UNIQUENESS
-- =========================

CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_trip_nk
ON fact_trip (
  pickup_date,
  pickup_time,
  pickup_location_id,
  dropoff_location_id,
  vendor_id
);

-- =========================
-- DIMENSION INDEXES
-- =========================

CREATE INDEX IF NOT EXISTS idx_dim_date_year_month
ON dim_date (year, month);

CREATE INDEX IF NOT EXISTS idx_dim_time_hour
ON dim_time (hour);
