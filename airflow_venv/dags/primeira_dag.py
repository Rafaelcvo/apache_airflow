from airflow import DAG
from datetime import datetime


with DAG(
    dag_id='selecao_acoes',
    description='Programa para selecionar acoes do Ibovespa',
    start_date=datetime(2022,5,13)
) as dag:
    pass