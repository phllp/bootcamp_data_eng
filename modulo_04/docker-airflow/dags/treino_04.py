from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
from zipfile import ZipFile
import os

url = 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip'

# DATA_FOLDER = os.getcwd()
DATA_FOLDER = '/home/phllp/dev/bootcamp_data_eng/modulo_04/docker-airflow/data'
FILE_NAME = 'microdados_enade_2019.zip'

print(os.getcwd())
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

def unzip_file():
  with ZipFile(DATA_FOLDER, 'r') as zipped:
    zipped.extractall(f'{DATA_FOLDER}')

def aplica_filtros():
  cols = ["CO_GRUPO", "TP_SEXO", "NU_IDADE", "NT_GER", "NT_FG", "NT_CE", "QE_IO1", "QE_IO2", "QE_IO4", "QE_IO5", "QE_IO8"]
  enade = pd.read_csv(f'')

dag = DAG(
  'treino_02',
  description='Extrai dados do titanic da internet e calcula a idade media',
  default_args=default_args,
  schedule_interval=None
)

start_preprocessing = BashOperator(
  task_id='start_preprocessing',
  bash_command='echo "[INFO] Preprocessing started"',
  dag=dag
)

get_data = BashOperator(
  task_id='get_data',
  bash_command=f'curl -k {url} -o {DATA_FOLDER}/{FILE_NAME}'
)

unzip_data = PythonOperator(
  task_id='unzip_data',
  python_callable= unzip_file,
  dag=dag
)

