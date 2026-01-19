-- EX03 - creation.sql
-- Création de la table de staging (créée uniquement par EX03, jamais par Spark)

CREATE SCHEMA IF NOT EXISTS public;

DROP TABLE IF EXISTS public.yellow_trips_staging;

CREATE TABLE public.yellow_trips_staging (
  vendorid               INTEGER,
  tpep_pickup_datetime   TIMESTAMP,
  tpep_dropoff_datetime  TIMESTAMP,
  passenger_count        INTEGER,
  trip_distance          DOUBLE PRECISION,
  ratecodeid             INTEGER,
  store_and_fwd_flag     TEXT,
  pulocationid           INTEGER,
  dolocationid           INTEGER,
  payment_type           INTEGER,
  fare_amount            DOUBLE PRECISION,
  extra                  DOUBLE PRECISION,
  mta_tax                DOUBLE PRECISION,
  tip_amount             DOUBLE PRECISION,
  tolls_amount           DOUBLE PRECISION,
  improvement_surcharge  DOUBLE PRECISION,
  total_amount           DOUBLE PRECISION,
  congestion_surcharge   DOUBLE PRECISION,
  airport_fee            DOUBLE PRECISION
);

