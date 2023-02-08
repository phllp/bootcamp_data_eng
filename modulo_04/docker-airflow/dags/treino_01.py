from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

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

# Definindo a DAG

dag = DAG(
  'treino_01',
  description='Basico de BashOperator e PythonOperator',
  default_args=default_args,
  schedule_interval='@daily'
)

# Comecando a adicionar tarefas

hello_bash = BashOperator(
  task_id='hello_bash',
  bash_command='echo "Hello Airflow from bash."',
  dag=dag
)

def say_hello():
  print('Hello Airflow from Python')

hello_python = PythonOperator(
  task_id='hello_python',
  python_callable=say_hello,
  dag=dag
)

hello_bash >> hello_python











