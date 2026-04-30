#!/bin/bash
set -e
<<<<<<< HEAD
echo "Creating MediStream HDFS zones..."
=======

echo "Creating MediStream HDFS zones..."

# Landing zone — replication 3 (HIPAA-relevant)
>>>>>>> 2f7ee623795e08618be180335190b4dd97824c3c
hdfs dfs -mkdir -p /medistream/landing/appointments
hdfs dfs -mkdir -p /medistream/landing/patient_vitals
hdfs dfs -mkdir -p /medistream/landing/session_quality
hdfs dfs -mkdir -p /medistream/landing/patient_feedback
hdfs dfs -mkdir -p /medistream/landing/physician_schedule
<<<<<<< HEAD
=======
hdfs dfs -setrep -R 3 /medistream/landing

# Curated zone — replication 2
>>>>>>> 2f7ee623795e08618be180335190b4dd97824c3c
hdfs dfs -mkdir -p /medistream/curated/appointments
hdfs dfs -mkdir -p /medistream/curated/patient_vitals
hdfs dfs -mkdir -p /medistream/curated/session_quality
hdfs dfs -mkdir -p /medistream/curated/patient_feedback
hdfs dfs -mkdir -p /medistream/curated/physician_schedule
<<<<<<< HEAD
=======
hdfs dfs -setrep -R 2 /medistream/curated

# Analytics zone — replication 1
>>>>>>> 2f7ee623795e08618be180335190b4dd97824c3c
hdfs dfs -mkdir -p /medistream/analytics/no_show_features
hdfs dfs -mkdir -p /medistream/analytics/physician_perf
hdfs dfs -mkdir -p /medistream/analytics/session_quality_agg
hdfs dfs -mkdir -p /medistream/analytics/utilization_rates
hdfs dfs -mkdir -p /medistream/analytics/patient_journey
<<<<<<< HEAD
hdfs dfs -setrep -R 2 /medistream/landing
hdfs dfs -setrep -R 2 /medistream/curated
hdfs dfs -setrep -R 1 /medistream/analytics
hdfs dfs -chmod -R 777 /medistream
echo "Done. Zone structure:"
=======
hdfs dfs -setrep -R 1 /medistream/analytics

# Permissions for all team members
hdfs dfs -chmod -R 777 /medistream

>>>>>>> 2f7ee623795e08618be180335190b4dd97824c3c
hdfs dfs -ls -R /medistream
