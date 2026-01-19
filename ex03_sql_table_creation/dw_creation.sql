-- =========================
-- DIMENSIONS
-- =========================

DROP TABLE IF EXISTS dim_date CASCADE;
CREATE TABLE dim_date (
  date_id DATE PRIMARY KEY,
  year INTEGER,
  month INTEGER,
  day INTEGER,
  day_of_week INTEGER
);

DROP TABLE IF EXISTS dim_time CASCADE;
CREATE TABLE dim_time (
  time_id TIME PRIMARY KEY,
  hour INTEGER,
  minute INTEGER
);

DROP TABLE IF EXISTS dim_location CASCADE;

CREATE TABLE dim_location (
  location_id INTEGER PRIMARY KEY,
  borough TEXT,
  zone TEXT,
  service_zone TEXT
);


DROP TABLE IF EXISTS dim_vendor CASCADE;

CREATE TABLE dim_vendor (
  vendor_id INTEGER PRIMARY KEY,
  vendor_name TEXT
);


DROP TABLE IF EXISTS dim_payment_type CASCADE;

CREATE TABLE dim_payment_type (
  payment_type_id INTEGER PRIMARY KEY,
  payment_description TEXT
);

DROP TABLE IF EXISTS dim_ratecode CASCADE;

CREATE TABLE dim_ratecode (
  ratecode_id INTEGER PRIMARY KEY,
  ratecode_description TEXT
);


-- =========================
-- FACT TABLE
-- =========================

DROP TABLE IF EXISTS fact_trip;

CREATE TABLE fact_trip (
  trip_id BIGSERIAL PRIMARY KEY,

  pickup_date DATE REFERENCES dim_date(date_id),
  pickup_time TIME REFERENCES dim_time(time_id),

  pickup_location_id INTEGER REFERENCES dim_location(location_id),
  dropoff_location_id INTEGER REFERENCES dim_location(location_id),

  vendor_id INTEGER REFERENCES dim_vendor(vendor_id),
  payment_type_id INTEGER REFERENCES dim_payment_type(payment_type_id),
  ratecode_id INTEGER REFERENCES dim_ratecode(ratecode_id),

  passenger_count INTEGER,
  trip_distance DOUBLE PRECISION,

  fare_amount DOUBLE PRECISION,
  extra DOUBLE PRECISION,
  mta_tax DOUBLE PRECISION,
  tip_amount DOUBLE PRECISION,
  tolls_amount DOUBLE PRECISION,
  improvement_surcharge DOUBLE PRECISION,
  congestion_surcharge DOUBLE PRECISION,
  airport_fee DOUBLE PRECISION,
  total_amount DOUBLE PRECISION
);
