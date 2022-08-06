# Import das bibliotecas do airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Passando as configuracoes do Dag
default_args = {
    'owner': 'rafael',
    'depends_on_past': False,
    # Exemplo: Inicia em 20 de Janeiro de 2021
    'start_date': datetime(2021, 1, 20),
    'email': ['*@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # Em caso de erros, tente rodar novamente apenas 1 vez
    'retries': 1,
    # Tente novamente após 30 segundos depois do erro
    'retry_delay': timedelta(seconds=30),
    # Execute uma vez a cada 15 minutos 
    'schedule_interval': '*/15 * * * *'
}

with DAG(    
    dag_id='hello_word',
    default_args=default_args,
    schedule_interval=None,
    tags=['hello'],
) as dag:
