-- ===========================================================================
-- hive/create_tables.hql
-- PPT layer 3 - APACHE HIVE: a SQL "data warehouse" view over the data lake.
--
-- These are EXTERNAL tables: Hive only stores the table definition (in the
-- Hive metastore); the data stays in the Parquet files that Spark writes.
-- Dropping a table therefore never deletes data.
--
-- ${DATA_DIR} is replaced by the runner script with the data lake folder.
-- Run with:  docker compose run --rm hive-queries
-- ===========================================================================

CREATE DATABASE IF NOT EXISTS wearable_health COMMENT 'Wearable device health monitoring data lake';
USE wearable_health;

-- Patients table (PPT: "Patients Table") - master list written by the simulator
DROP TABLE IF EXISTS patients;
CREATE TABLE patients (
    patient_id  STRING,
    age         INT,
    gender      STRING,
    ward        STRING,
    device_type STRING,
    device_id   STRING
)
USING CSV
OPTIONS (header 'true')
LOCATION '${DATA_DIR}/reference/patients';

-- BRONZE: raw Kafka messages (raw JSON)
DROP TABLE IF EXISTS raw_vitals;
CREATE TABLE raw_vitals USING PARQUET LOCATION '${DATA_DIR}/bronze/raw_vitals';

-- SILVER: cleaned vitals (PPT: "Vitals Table")
DROP TABLE IF EXISTS vitals;
CREATE TABLE vitals USING PARQUET LOCATION '${DATA_DIR}/silver/vitals';

-- GOLD: 1-minute patient features
DROP TABLE IF EXISTS vitals_1min;
CREATE TABLE vitals_1min USING PARQUET LOCATION '${DATA_DIR}/gold/vitals_1min';

-- GOLD: threshold alerts (PPT: "Alerts Table")
DROP TABLE IF EXISTS alerts;
CREATE TABLE alerts USING PARQUET LOCATION '${DATA_DIR}/gold/alerts';

-- GOLD: MLlib risk level per patient (exists after Phase 6 / the ML job has run)
DROP TABLE IF EXISTS patient_risk;
CREATE TABLE patient_risk USING PARQUET LOCATION '${DATA_DIR}/gold/patient_risk';
