-- ===========================================================================
-- hive/queries.hql
-- Example HiveQL queries over the Bronze / Silver / Gold tables.
-- Each query starts with a "-- Qn:" title line (the runner prints it).
-- You can also paste any of these into the interactive Hive shell:
--     docker compose run --rm hive-shell
-- ===========================================================================

-- Q1: Average vitals per patient (Silver layer joined with the Patients table)
SELECT v.patient_id,
       p.ward,
       COUNT(*)                        AS readings,
       ROUND(AVG(v.heart_rate), 1)     AS avg_hr,
       ROUND(AVG(v.spo2), 1)           AS avg_spo2,
       ROUND(AVG(v.systolic_bp), 0)    AS avg_sys,
       ROUND(AVG(v.diastolic_bp), 0)   AS avg_dia,
       ROUND(AVG(v.body_temp), 2)      AS avg_temp
FROM vitals v
LEFT JOIN patients p ON v.patient_id = p.patient_id
GROUP BY v.patient_id, p.ward
ORDER BY v.patient_id;

-- Q2: Alert counts by type and severity, with average alert latency
SELECT alert_type,
       severity,
       COUNT(*)                    AS alerts,
       COUNT(DISTINCT patient_id)  AS patients_affected,
       ROUND(AVG(latency_sec), 2)  AS avg_latency_sec
FROM alerts
GROUP BY alert_type, severity
ORDER BY alerts DESC;

-- Q3: Top 5 risky patients (most critical alerts, then most alerts)
SELECT a.patient_id,
       p.age,
       p.ward,
       COUNT(*)                                              AS total_alerts,
       SUM(CASE WHEN a.severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_alerts,
       concat_ws(', ', collect_set(a.alert_type))            AS alert_types
FROM alerts a
LEFT JOIN patients p ON a.patient_id = p.patient_id
GROUP BY a.patient_id, p.age, p.ward
ORDER BY critical_alerts DESC, total_alerts DESC
LIMIT 5;

-- Q4: Hourly trend per ward (cohort query on the Gold 1-minute features)
SELECT p.ward,
       date_format(w.window_start, 'yyyy-MM-dd HH:00')  AS hour_utc,
       COUNT(DISTINCT w.patient_id)                       AS patients,
       ROUND(AVG(w.avg_hr), 1)                            AS avg_hr,
       ROUND(AVG(w.avg_spo2), 2)                          AS avg_spo2,
       SUM(w.abnormal_readings)                           AS abnormal_readings
FROM vitals_1min w
JOIN patients p ON w.patient_id = p.patient_id
GROUP BY p.ward, date_format(w.window_start, 'yyyy-MM-dd HH:00')
ORDER BY hour_utc, p.ward;

-- Q5: Daily summary per patient - lowest oxygen first (who needs attention today?)
SELECT event_date,
       patient_id,
       COUNT(*)                 AS minutes_monitored,
       ROUND(AVG(avg_hr), 1)    AS avg_hr,
       MAX(max_hr)              AS peak_hr,
       MIN(min_spo2)            AS lowest_spo2,
       MAX(max_temp)            AS highest_temp,
       SUM(falls)               AS falls
FROM vitals_1min
GROUP BY event_date, patient_id
ORDER BY lowest_spo2 ASC, peak_hr DESC
LIMIT 10;

-- Q6: Data quality - how many raw messages the noise filter dropped (Bronze vs Silver)
SELECT b.bronze_messages,
       s.silver_clean_readings,
       b.bronze_messages - s.silver_clean_readings  AS dropped_or_pending
FROM (SELECT COUNT(*) AS bronze_messages FROM raw_vitals) b
CROSS JOIN (SELECT COUNT(*) AS silver_clean_readings FROM vitals) s;

-- Q7: Patient risk levels predicted by Spark MLlib (needs Phase 6 to have run)
SELECT risk_level,
       COUNT(*)                                  AS patients,
       concat_ws(', ', sort_array(collect_list(patient_id))) AS patient_ids
FROM patient_risk
GROUP BY risk_level
ORDER BY CASE risk_level WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END;
