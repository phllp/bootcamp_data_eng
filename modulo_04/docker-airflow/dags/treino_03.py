from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
import os

# DATA_FOLDER = os.getcwd()
DATA_FOLDER = '/home/phllp/dev/bootcamp_data_eng/modulo_04/docker-airflow/data/train.csv'

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

def calculate_mean_age():
  df = pd.read_csv(f'{DATA_FOLDER}/train.csv')
  med = df['Age'].mean()
  return med

def print_age(**context):
  sex = context['task_instance'].xcom_pull(task_ids='sort_sex')

  if sex == 'male':
    task = 'male_branch'
  elif sex == 'female':
    task = 'female_branch'
  value = context['task_instance'].xcom_pull(task_ids=task)
  print(f'A idade media no Titanic era {value} anos')

def handle_sex(**context):
  value = context['task_instance'].xcom_pull(task_ids='sort_sex')
  if value == 'male':
    return 'male_branch'
  if value == 'female':
    return 'female_branch'

def sorteia_h_m():
  return random.choice(['male', 'female'])

def calculate_mean_age_male():
  df = pd.read_csv(f'{DATA_FOLDER}/train.csv')
  med = df.loc[df['Sex'] == 'male']['Age'].mean()
  return med

def calculate_mean_age_female():
  df = pd.read_csv(f'{DATA_FOLDER}/train.csv')
  med = df.loc[df['Sex'] == 'female']['Age'].mean()
  return med

dag = DAG(
  'treino_03',
  description='Extrai dados do titanic da internet e calcula a idade media para homens ou mulheres',
  default_args=default_args,
  schedule_interval=None
)

get_data = BashOperator(
  task_id='get_data',
  bash_command=F'curl -k https://raw.githubusercontent.com/phllp/public_datasets/main/train.csv -o {DATA_FOLDER}',
  dag=dag
)

sort_sex = PythonOperator(
  task_id='sort_sex',
  python_callable=sorteia_h_m,
  dag=dag
)

handle_sex_branch = BranchPythonOperator(
  task_id='handle_sex_branch',
  python_callable=handle_sex,
  provide_context=True,
  dag=dag
)

male_branch = PythonOperator(
  task_id='male_branch',
  python_callable=calculate_mean_age_male,
  dag=dag
)

female_branch = PythonOperator(
  task_id='female_branch',
  python_callable=calculate_mean_age_female,
  dag=dag
)

print_mean_age = PythonOperator(
  task_id='print_mean_age',
  python_callable=print_age,
  provide_context=True,
  dag=dag
)


get_data >> sort_sex >> handle_sex_branch >> [male_branch, female_branch]
male_branch >> print_mean_age
female_branch >> print_mean_age
