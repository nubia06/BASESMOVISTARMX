# Importar librerias necesarias
import os
import sys
from Functions import *
from Connections import *
sys.path.append(os.path.join(".."))
from Imports import *


# Variables globales para el proceso
inicio  = time.time()
anio    = datetime.datetime.now().strftime("%Y")
mes     = datetime.datetime.now().strftime("%m")
dia     = datetime.datetime.now().strftime("%d")
hora    = datetime.datetime.now().strftime("%H:%M")
fecha   = datetime.datetime.now().strftime("%Y-%m-%d")

def Read_files_path(path_,nombre_tabla,nombre_archivo):
    try: 
        print(nombre_tabla)
        archivos_total = os.listdir(path_) 
        print(archivos_total)
        archivos = [valor for valor in archivos_total if f"{nombre_archivo}" in valor]
        print(f"{archivos}\n")
    except Exception as e:
        logging.getLogger("user").exception(e)
        raise 
    with open(f"//root//PROCESOS//BASESMOVISTAR//src//LoadedFiles//{nombre_tabla}.log", "r") as f:
        loaded_files = f.read().splitlines()  # todo el listado 
        # Obtener la lista de archivos con fechas de modificación
        archivos_con_fecha = [f"{datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(path_, archivo))).strftime('%Y-%m-%d %H:%M:%S')} - {archivo}" for archivo in archivos]
        different_files = set(archivos_con_fecha).difference(loaded_files)  # Comparación de los archivos ya cargados y los archivos para cargar así cargar las diferencias 
    if len(different_files) == 0:
        logging.getLogger("user").debug("No hay archivos para cargar")
        print(different_files)
    return different_files




def toSqlTxt(path,nombre_tabla,file_, dic_fechas,dic_formatos,separador):
    print(dic_fechas)
    print(dic_formatos)
    time.sleep(999)
    logging.info('Lectura archivo')
    print(file_)
    df = pd.read_csv(path+"/"+file_, dtype=str, index_col=False)
    fecha= datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(path, file_)))
    df['FILE_DATE'] = fecha
    df['FILE_NAME'] = file_
    df['FILE_YEAR'] = df['FILE_DATE'].dt.year
    df['FILE_MONTH']  = df['FILE_DATE'].dt.month
    columnas_r = ["CUENTA"]
    for cr in columnas_r:
        if cr in df.columns:
            df[cr] = df[cr].str.replace(",", ".", regex=True)
            df[cr] = df[cr].str.replace("[^0-9-.]", "", regex=True)
    engine,bbdd_or = mysql_engine()
    connection,cdn_connection = mysql_connection()
    try:
        tabla = Table(f"tb_asignacion_{nombre_tabla}", MetaData(), autoload_with = engine)
        nombre_columnas_nuevas = [c.name for c in tabla.c]
        diffCols = df.columns.difference(nombre_columnas_nuevas)
        listCols = list(diffCols)
        if len(listCols)!=0:
            for i in range(len(listCols)):
                logging.getLogger("user").info(f"Columna {i} agregada.")
                connection.execute(text(f"ALTER TABLE `{bbdd_or}`.`{tabla.name}` ADD COLUMN `" + listCols[i] + "` VARCHAR(128)"))
        tabla = Table(f"tb_asignacion_{nombre_tabla}", MetaData(), autoload_with = engine)
        columnas_nuevas = [Column(c.name, c.type) for c in tabla.c]
        tmp = Table(f"{tabla.name}_tmp", MetaData(), *columnas_nuevas)
        tmp.drop(bind = engine,checkfirst=True)
        tmp.create(bind = engine)
        logging.getLogger("user").info('Creacion Tabla temporal')
        # Cargue del DataFrame de Dask en la base de datos MySQL.
        df.to_sql(tmp.name, connection, if_exists='append',index=False)
        connection.execute(text(f"INSERT IGNORE INTO {tabla.name} SELECT * FROM {tmp.name};"))
        tmp.drop(bind = engine,checkfirst=True)
        logging.getLogger("user").info('Insercion Tabla destino DW')
    except Exception as e:
        print(f"ERROR {e}")
        logging.getLogger("user").exception(e)
        raise








def check_and_add(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador):
    print(file)
    ini = time.time()
    LOADED_FILES = f"//root//PROCESOS//BASESMOVISTAR//src//LoadedFiles//{nombre_tabla}.log"

    with open(LOADED_FILES, "r") as f:
        logs     = f.read().splitlines()
        file_tmp = f.read().splitlines()
    
    logging.getLogger("user").info(f"La base '{file[22:]}' del {file[:20]} esta por cargarse")
    logs.append(file)
    with open(LOADED_FILES, "w") as f:
            f.write(f"\n".join(logs))
    try:
        nombre, extension = os.path.splitext(file)
        print(extension)
        if extension in [".txt", ".csv"]:
            print("txt")
            toSqlTxt(path,nombre_tabla,file, dic_fechas,dic_formatos,separador)
        elif extension in [".xlsx"]:
            print("excel")
            toSqlExcel(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador)
        elif extension in [".zip", ".ZIP"]:
            toSqlZip(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador)
        send(f"Se acaba de cargar la base {file[22:]} de Claro Cobranza ")
        logging.getLogger("user").info(f"Base '{file[22:]}' del {file[:20]} recien cargada [ToSQL]")

    except Exception as e:
        with open(LOADED_FILES, "w") as f:
            f.write("\n".join(file_tmp))
        logging.getLogger("user").exception(f"Error en archivo: {file}\n{e}")
        send(f"Error cargando la base {file}: {e}")
    finally:
        fin = time.time()
        logging.getLogger("user").info(f"Tiempo total de ejecucion: {ini-fin} de {file}")

def scan_folder(path,nombre_tabla,nombre_archivo,dic_fechas,dic_formatos,dic_hojas,separador):
    try:
        if os.path.exists(path):
            file_to_load = Read_files_path(path,nombre_tabla,nombre_archivo)
            for file in file_to_load:
                print("cargando",file)
                check_and_add(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador)
        else:
            if hora == '08:00':
                send(f"No ha habido cargue de la base {nombre_tabla} del {anio}-{mes}-{dia}")
         
    except Exception as e:
        print(e)