-- EX03 - insertion.sql
-- Script de démonstration d'insertion dans la table de staging
-- (En exécution réelle du pipeline, l'insertion est faite par Spark via JDBC - EX02 branche 2)

-- Exemple minimal : 1 ligne de test
INSERT INTO public.yellow_trips_staging (
  vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
  ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type,
  fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
  total_amount, congestion_surcharge, airport_fee
) VALUES (
  1, '2023-01-01 00:00:00', '2023-01-01 00:10:00', 1, 1.0,
  1, 'N', 100, 200, 1,
  10.0, 1.0, 0.5, 2.0, 0.0, 1.0,
  14.5, 2.5, 0.0
);

-- =========================
-- DIMENSIONS DE RÉFÉRENCE
-- =========================

INSERT INTO dim_vendor (vendor_id)
SELECT DISTINCT vendorid
FROM yellow_trips_staging
WHERE vendorid IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO dim_payment_type (payment_type_id)
SELECT DISTINCT payment_type
FROM yellow_trips_staging
WHERE payment_type IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO dim_ratecode (ratecode_id)
SELECT DISTINCT ratecodeid
FROM yellow_trips_staging
WHERE ratecodeid IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO dim_location (location_id)
SELECT DISTINCT pulocationid
FROM yellow_trips_staging
WHERE pulocationid IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO dim_location (location_id)
SELECT DISTINCT dolocationid
FROM yellow_trips_staging
WHERE dolocationid IS NOT NULL
ON CONFLICT DO NOTHING;

-- =========================
-- DIM DATE
-- =========================

INSERT INTO dim_date (date_id, year, month, day, day_of_week)
SELECT DISTINCT
  DATE(tpep_pickup_datetime),
  EXTRACT(YEAR FROM tpep_pickup_datetime),
  EXTRACT(MONTH FROM tpep_pickup_datetime),
  EXTRACT(DAY FROM tpep_pickup_datetime),
  EXTRACT(DOW FROM tpep_pickup_datetime)
FROM yellow_trips_staging
ON CONFLICT DO NOTHING;

-- =========================
-- DIM TIME
-- =========================

INSERT INTO dim_time (time_id, hour, minute)
SELECT DISTINCT
  CAST(tpep_pickup_datetime AS TIME),
  EXTRACT(HOUR FROM tpep_pickup_datetime),
  EXTRACT(MINUTE FROM tpep_pickup_datetime)
FROM yellow_trips_staging
ON CONFLICT DO NOTHING;


-- =========================
-- FACT TABLE
-- =========================

INSERT INTO fact_trip (
  pickup_date,
  pickup_time,
  pickup_location_id,
  dropoff_location_id,
  vendor_id,
  payment_type_id,
  ratecode_id,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  congestion_surcharge,
  airport_fee,
  total_amount
)
SELECT
  DATE(tpep_pickup_datetime),
  CAST(tpep_pickup_datetime AS TIME),
  pulocationid,
  dolocationid,
  vendorid,
  payment_type,
  ratecodeid,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  congestion_surcharge,
  airport_fee,
  total_amount
FROM yellow_trips_staging
ON CONFLICT DO NOTHING;


