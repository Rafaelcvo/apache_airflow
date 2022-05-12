# Tutorial Dag youtube https://www.youtube.com/watch?v=4DGRqMoyrPk&t=461s

from urllib import response
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import pandas as pd
import requests
import json

def captura_conta_dados():
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len(df.index)
    return qtd

def e_valido(ti):
    qtd =  ti.xcom_pull(task_ids = 'captura_conta_dados')
    if(qtd > 10):
        return 'valido'
    return 'n_valido'


with DAG('tutorial_dag', start_date= datetime(2021, 12, 1),
        schedule_interval='30 * * * *', catchup=False) as dag:
    
    captura_conta_dados = PythonOperator(
        task_id = 'captura_conta_dados',
        python_callable = captura_conta_dados
    )

    e_valido = BranchPythonOperator(
        task_id = 'e_valido',
        python_callable = e_valido
    )

    valido = BashOperator(
        task_id = 'valido',
        bash_command = "echo 'Quantidade Ok' "
    )  
    
    n_valido = BashOperator(
        task_id = 'n_valido',
        bash_command = "echo 'Quantidade não Ok' "
    )

    captura_conta_dados >> e_valido >> [valido, n_valido]