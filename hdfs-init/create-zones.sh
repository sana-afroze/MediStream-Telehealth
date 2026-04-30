#!/bin/bash
set -e
echo "Creating MediStream HDFS zones..."
hdfs dfs -mkdir -p /medistream/landing/appointments
hdfs dfs -mkdir -p /medistream/landing/patient_vitals
hdfs dfs -mkdir -p /medistream/landing/session_quality
hdfs dfs -mkdir -p /medistream/landing/patient_feedback
hdfs dfs -mkdir -p /medistream/landing/physician_schedule
hdfs dfs -mkdir -p /medistream/curated/appointments
hdfs dfs -mkdir -p /medistream/curated/patient_vitals
hdfs dfs -mkdir -p /medistream/curated/session_quality
hdfs dfs -mkdir -p /medistream/curated/patient_feedback
hdfs dfs -mkdir -p /medistream/curated/physician_schedule
hdfs dfs -mkdir -p /medistream/analytics/no_show_features
hdfs dfs -mkdir -p /medistream/analytics/physician_perf
hdfs dfs -mkdir -p /medistream/analytics/session_quality_agg
hdfs dfs -mkdir -p /medistream/analytics/utilization_rates
hdfs dfs -mkdir -p /medistream/analytics/patient_journey
hdfs dfs -setrep -R 2 /medistream/landing
hdfs dfs -setrep -R 2 /medistream/curated
hdfs dfs -setrep -R 1 /medistream/analytics
hdfs dfs -chmod -R 777 /medistream
echo "Done. Zone structure:"
hdfs dfs -ls -R /medistream
