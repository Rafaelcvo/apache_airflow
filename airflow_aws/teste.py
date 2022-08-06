import pandas as pd
import requests
import json


def captura_dados():
    url = 'https://data.cityofnewyork.us/resource/rc75-m7u3.json'
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len(df.index)
    return qtd

qut = captura_dados()
print(qut)