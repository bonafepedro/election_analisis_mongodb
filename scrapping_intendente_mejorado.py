import requests
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

def get_info(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None

def execute():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['eleccion_2023']
    collection = db['resultados_intendente_capital']

    # Consulta la colección "ubicaciones" para obtener los números de mesa y establecimiento
    ubicaciones_collection = db['ubicaciones']
    mesas_data = ubicaciones_collection.find({"clase_ubicacion": "Mesa"},
                                             {"_id": 0, "id_ubicacion": 1}) # el segundo parametro es una proyeccion para solo ver id_ubicacion
    
    base_url = "https://cordobaciudad.datosoficiales.com/resultados/"
    batch_size = 100  # Define el tamaño del lote para operaciones por lotes
    batch = []  # Lista para almacenar documentos antes de insertar en la base de datos

    for mesa_data in mesas_data:
        numero_mesa = mesa_data["id_ubicacion"]
        try:
            url = f"{base_url}{numero_mesa.replace('.', '/')}/IVC.json"
            data = get_info(url)
            if data:
                batch.append(data)
                if len(batch) >= batch_size:
                    # Realiza la operación por lotes en la base de datos
                    collection.insert_many(batch, ordered=False)
                    batch = []  # Reinicia el lote para la siguiente operación
        except Exception as e:
            print(f"Error en la solicitud {url}: {e}")
            continue

    # Inserta cualquier documento restante en la lista batch
    if batch:
        try:
            collection.insert_many(batch, ordered=False)
        except BulkWriteError as bwe:
            print(f"Error de inserción en lote: {bwe.details}")

if __name__ == "__main__":
    execute()
