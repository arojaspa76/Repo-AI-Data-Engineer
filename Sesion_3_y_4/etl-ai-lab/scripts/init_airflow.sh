#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."

python3 -m venv .venv
source .venv/bin/activate
pip install -r airflow/requirements.txt

export AIRFLOW_HOME="$(pwd)/airflow"
airflow db init

airflow users create \
  --username admin \
  --firstname Andres \
  --lastname Rojas \
  --role Admin \
  --email admin@triskel.ai \
  --password admin123

airflow webserver --port 8080 &
sleep 5
airflow scheduler &
