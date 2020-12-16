from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Argumentos padrão
default_args = {
    'owner': 'Gustavo Gonzaga',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 15, 21),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'dag_etl_imea_soja',
    description='DAG para scrap do IMEA da safra de soja com arquivo mais recente e persistencia em BD',
    default_args=default_args,
    schedule_interval=timedelta(days=7)
)

bash_extract_recent = BashOperator(
    task_id="extract_recent_file",
    bash_command="cd /home/gustavo/Documentos/Python\ Scripts/desafio_imea; python3 extract_recent.py",
    dag=dag
)

bash_transform = BashOperator(
    task_id="transform",
    bash_command="cd /home/gustavo/Documentos/Python\ Scripts/desafio_imea; python3 transform_v2.py",
    dag=dag
)

bash_load = BashOperator(
    task_id="load",
    bash_command="cd /home/gustavo/Documentos/Python\ Scripts/desafio_imea; python3 load.py",
    dag=dag
)

bash_extract_recent >> bash_transform >> bash_load