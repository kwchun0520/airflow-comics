export $(cat .env | xargs)
export AIRFLOW_HOME=$(pwd)
airflow webserver -p 8080 && airflow scheduler