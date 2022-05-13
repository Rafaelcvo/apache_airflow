from airflow import DAG
from datetime import datetime
import pandas as pd
import numpy as np
from urllib.request import urlopen
from pandas_datareader import data as wb
import cufflinks as cf
from urllib.request import urlopen, Request
cf.go_offline()
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def busca():
    r = Request('http://www.fundamentus.com.br/resultado.php', headers={'User-Agent': 'Mozilla/5.0'})
    html = urlopen(r).read()
    arq = pd.read_html(html)
    df = pd.DataFrame(arq[0])
    return df

with DAG(
    dag_id='selecao_acoes',
    description='Programa para selecionar acoes do Ibovespa',
    start_date=datetime(2022,5,13),
    scheule_interval='30 * * * *',
    catchup=False
) as dag:
    pass