import requests 
import json
from pymongo import MongoClient

def gettinf(url):
    """
    getting information from the electoral page
    """
    
    response = requests.get(url)
    if response.status_code == 200:
        results = response.json()
        return results

    return 404


def execute ():
    """
    This function execute all the program
    """

    #api_url = "https://cordoba.datosoficiales.com/resultados/CO/"

    # Configura la conexión a MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    
    # Selecciona la base de datos
    db = client['eleccion_2023']
    
    # Selecciona la colección
    collection = db['intendente_capital']

    for i in range(1, 501):
        for j in range(1, 100000):

            try:

                url =  f"https://cordobaciudad.datosoficiales.com/resultados/1/{i}/{j}/IVC.json"
                data = gettinf(url)
                #print(data)
                x = collection.insert_one(data)
                print(x)

            except:
                continue
            


if __name__ == "__main__":

    execute()

