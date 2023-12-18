# Importar librerias 
from Imports import *
# Importar ruta de controller
sys.path.append(os.path.join("Controllers"))
from controller import *

# Obterner la colección de rutas a consultar   
with open(os.path.join("config","config_files.json")) as archivo_config:
    configuracion = json.load(archivo_config)
archivos = configuracion["ejecuciones"].keys()
coleccion = configuracion["ejecuciones"]

# Iterar las rutas para poceder hacer el proceso de cada una de las rutas 
for i in range(len(list(archivos))):
    key1=list(archivos)[i].strip("'")
    keys = coleccion[key1].keys()
    for j in  range(len(list(archivos))):
        key2=list(keys)[j].strip("'")
        print(key2)
    nombre_archivo = coleccion[key1][0].strip("'")
    nombre_tabla   = coleccion[key1][1].strip("'")
    path_0         = coleccion[key1][2].strip("'")
    path           = f"{path_0}\{anio}\{mes}"

    if __name__ == '__main__':
        #scan_folder(path,nombre_tabla,nombre_archivo)
        print(path)
        print(nombre_archivo)