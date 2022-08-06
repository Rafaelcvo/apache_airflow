# Import das bibliotecas do airflow
from datetime import datetime, timedelta
from urllib import response
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import json

def captura_dados():
    url = 'https://data.cityofnewyork.us/resource/rc75-m7u3.json'
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len(df.index)
    return qtd

def e_valido(ti):
    qtd = ti.xcom_pull(task_ids = 'captura_dados')
    if(qtd > 1000):
        return 'valido'
    return 'nvalido'


# Passando as configuracoes do Dag
default_args = {
    'owner': 'rafael',
    'depends_on_past': False,
    # Exemplo: Inicia em 20 de Janeiro de 2021
    'start_date': datetime(2021, 1, 20),
    'catchup': False,
    'retries': 1,
    # Tente novamente após 30 segundos depois do erro
    'retry_delay': timedelta(seconds=30),
    # Execute uma vez a cada 15 minutos 
    'schedule_interval': '* * * * * *'
}

with DAG(    
    dag_id='captura_dados',
    default_args=default_args,
    schedule_interval=None,
    tags=['captura'],
) as dag:
    captura_dados = PythonOperator(
        task_id = 'captura_dados', 
        python_callable = captura_dados
    )

    e_Valido = BranchPythonOperator(
        task_id = 'e_valido',
        python_callable = e_valido
    )

    valido = BashOperator(
        task_id = 'valido',
        bash_command = "echo 'Quantidade OK'"
    )

    nvalido = BashOperator(
        task_id = 'nvalido',
        bash_command = "echo 'Quantidade nao OK'"
    )

    captura_dados >> e_Valido >> [valido, nvalido]
