# create venv with python 3.10  
python3.10 -m venv .venv
activate venv  

source .venv/bin/activate

# install dependencies
pip install -r requirements.txt 

# set up airflow 
create database postgres

psql -U postgres -h localhost

password =  postgres (depend on you)

CREATE DATABASE "airflowRetailETLProject"
  WITH OWNER = postgres
       ENCODING = 'UTF8'
       CONNECTION LIMIT = -1;

return to the cmd screen 

cd to airflow folder

export AIRFLOW_HOME=~/retailETLProject/airflow/

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@localhost/airflowRetailETLProject

airflow db init

success if the airflow.cfg file appear 

airflow users create \
    --username admin \
    --firstname YourFirstName \
    --lastname YourLastName \
    --role Admin \
    --email admin@example.com \
    --password admin

check the sql_alchemy_conn variable again

then start the web server 

airflow webserver -p 8080 , to start the webserver 

airflow scheduler , to start the scheduler

access the localhost:8080 to see the result 

now, you can see many example dags, you don't need it, come back to the airflow.cfg file
change this row to False: 

load_examples = False

then , restart the scheduler

# design snowflake schema

dim_address
dim_date
dim_order
dim_customer
dim_product
fact_detail_order

chose scd type 2 to keep track of the history of dimension

setting clickhouse 
