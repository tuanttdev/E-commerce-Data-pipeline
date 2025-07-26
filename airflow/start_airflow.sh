#!/bin/bash
# File này được build trên nền là wsl2 + ubuntu
# Lệnh explorer.exe là của window mở windows explorer chứ không phải lệnh gốc của unbuntu
# Đường dẫn virtualenv
echo "Activate venv ..."

AIRFLOW_VENV="$HOME/retailETLProject/.venv"
export AIRFLOW_HOME=~/retailETLProject/airflow/
source "$AIRFLOW_VENV/bin/activate"
export AIRFLOW_CONFIG=$AIRFLOW_HOME/airflow.cfg


export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@localhost/airflowRetailETLProject

echo "Starting Airflow webserver..."
airflow webserver --port 8080 > $PWD/airflow/logs/airflow-webserver.log 2>&1 &

echo "Starting Airflow scheduler..."
airflow scheduler > $PWD/airflow/logs/airflow-scheduler.log 2>&1 &

while ! nc -z localhost 8080; do
  echo "Waiting for Airflow Webserver..."
  sleep 2
done

# Mở Airflow Web UI trên trình duyệt
echo "Opening Airflow Web UI..."
explorer.exe http://localhost:8080

deactivate
