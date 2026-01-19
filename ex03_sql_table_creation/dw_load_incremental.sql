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

INSERT INTO dim_date (date_id, year, month, day, day_of_week)
SELECT DISTINCT
  DATE(tpep_pickup_datetime),
  EXTRACT(YEAR FROM tpep_pickup_datetime),
  EXTRACT(MONTH FROM tpep_pickup_datetime),
  EXTRACT(DAY FROM tpep_pickup_datetime),
  EXTRACT(DOW FROM tpep_pickup_datetime)
FROM yellow_trips_staging
ON CONFLICT DO NOTHING;


INSERT INTO dim_time (time_id, hour, minute)
SELECT DISTINCT
  CAST(tpep_pickup_datetime AS TIME),
  EXTRACT(HOUR FROM tpep_pickup_datetime),
  EXTRACT(MINUTE FROM tpep_pickup_datetime)
FROM yellow_trips_staging
ON CONFLICT DO NOTHING;


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
