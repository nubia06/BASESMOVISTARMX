# Importar las librerias neserias para el proceso
import sys
import os
sys.path.append(os.path.join(".."))
from Imports import *
ip = '60'
# conexión para obtener la conexion   
def mysql_connection():
    ruta_config_files = os.path.join("..", "config", f"config_{ip}.json")
    with open(ruta_config_files) as f:
        configuracion = json.load(f)
    credecial=configuracion["credenciales_conexion_db"]
    url       = f'mysql+pymysql://{credecial["user"]}:{quote(credecial["password"])}@{credecial["host"]}:{"3306"}/{credecial["database"]}'
    engine    = create_engine(url,pool_recycle=9600,isolation_level="AUTOCOMMIT")
    mysql_con = engine.connect()
    engine.dispose()
    return mysql_con,url

def mysql_engine():
    ruta_config_files = os.path.join("..", "config", "config_60.json")
    with open(ruta_config_files) as f:
        configuracion = json.load(f)
    credecial=configuracion["credenciales_conexion_db"]
    
    url       = f'mysql+pymysql://{credecial["user"]}:{quote(credecial["password"])}@{credecial["host"]}:{"3306"}/{credecial["database"]}'
    engine    = create_engine(url,pool_recycle=9600,isolation_level="AUTOCOMMIT")
    return engine, credecial["database"]
