from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

DATA_FOLDER = '/home/phllp/dev/bootcamp_data_eng/modulo_04/docker-airflow/data'
# Argumentos default
default_args = {
  'owner': 'felipe',
  'depends_on_past': False,
  'start_date': datetime(2023, 1, 27, 20),
  'email': ['random_mail@mail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries':1,
  'retry_delay': timedelta(minutes=1)
}


dag = DAG(
  'treino_02',
  description='Extrai dados do titanic da internet e calcula a idade media',
  default_args=default_args,
  schedule_interval='*/2 * * * *'
)

get_data = BashOperator(
  task_id='get_data',
  bash_command=F'curl http://127.0.0.1:5000/titanic-data -o {DATA_FOLDER}/train.csv',
  dag=dag
)

def calculate_mean_age():
  df = pd.read_csv(f'{DATA_FOLDER}/train.csv')
  med = df['Age'].mean()
  return med

def print_age(**context):
  value = context['task_instance'].xcom_pull(task_ids='calcula_idade_media')
  print(f'A idade media no Titanic era {value} anos')

calcula_idade_media = PythonOperator(
  task_id = 'calcula_idade_media',
  python_callable=calculate_mean_age,
  dag=dag
)

print_mean_age = PythonOperator(
  task_id='print_mean_age',
  python_callable=print_age,
  dag=dag
)


get_data >> calcula_idade_media >> print_mean_age