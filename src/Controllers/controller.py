# Importar librerias necesarias
import os
import sys
import pandas as pd

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
        print("BUSCANDO ARCHIVOS")
        archivos_coincidentes =[]
        print(nombre_archivo)
        archivos_total = os.listdir(path_) 
        for a in nombre_archivo:
            archivos = [valor for valor in archivos_total if f"{a}" in valor]
            archivos_coincidentes.extend(archivos)
        print(f"cantidad archivos: {archivos_coincidentes}\n")
    except Exception as e:
        print("error")
        logging.getLogger("user").exception(e)
        raise 
    with open(f"//root//PROCESOS//BASESMOVISTAR//src//LoadedFiles//{nombre_tabla}.log", "r") as f:
        loaded_files = f.read().splitlines()  # todo el listado 
        # Obtener la lista de archivos con fechas de modificación
        archivos_con_fecha = [f"{datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(path_, archivo))).strftime('%Y-%m-%d %H:%M:%S')} - {archivo}" for archivo in archivos_coincidentes]
        different_files = set(archivos_con_fecha).difference(loaded_files)  # Comparación de los archivos ya cargados y los archivos para cargar así cargar las diferencias 
    if len(different_files) == 0:
        logging.getLogger("user").debug("No hay archivos para cargar")
        print(different_files)
    return different_files

def insertar_raya_al_piso(cadena): #Funcion que inserta raya al piso cuando el encabezado es CamelCase
        nueva_cadena=""
        try:
            if cadena.isupper() or cadena.islower() or ("_" in cadena):
                nueva_cadena=cadena
            else:
                i=0
                for letra in cadena:
                    if letra.isupper() and i!=0:
                        nueva_cadena =nueva_cadena + "_" + letra
                    else:
                        nueva_cadena = nueva_cadena + letra
                    i+=1
            return nueva_cadena
        except:
            return cadena

def convertir_fecha(fecha):
    try:
        fecha=pd.to_datetime(fecha, format="%Y-%m-%d %I:%M:%S")
    except:
        try:
            fecha= pd.to_datetime(fecha, format="%Y-%m-%d")
        except:
            try:
                fecha=pd.to_datetime(fecha, format="%Y/%m/%d")
            except:
                fecha=pd.to_datetime(fecha, format="%d/%m/%Y")

    return fecha
            

def toSqlTxt(path,nombre_tabla,file_, dic_fechas, dic_formatos, separador):
    print("EN EL TO SQL PARA TEXTOS")
    print(dic_formatos)
    logging.info('Lectura archivo')
    print(file_)
    df = pd.read_csv(path+"/"+file_[22:],sep = separador,dtype=str, index_col=False)
    print(df)
    fecha= datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(path, file_[22:])))
    tabla_reemplazo = str.maketrans({"á":"a","é":"e","í":"i","ó":"o","ú":"u","ñ":"n","Á":"A","É":"E","Í":"I","Ó":"O","Ú":"U","Ñ":"N"})
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(" ", "_", regex=True)
    df.columns = df.columns.str.translate(tabla_reemplazo)
    df.columns = df.columns.str.replace("[^0-9a-zA-Z_]", "", regex=True)
    df.columns = [insertar_raya_al_piso(nombre_columna) for nombre_columna in df.columns]
    df.columns = df.columns.str.upper()
    print(dic_fechas)
    for clave in dic_fechas:
        valor = dic_fechas[clave]
        print(valor)
        if valor[0] in df.columns:
            df[valor[0]] = df[valor[0]].apply(convertir_fecha)
           
    df['FILE_DATE'] = fecha
    df['FILE_NAME'] = file_
    df['FILE_YEAR'] = df['FILE_DATE'].dt.year
    df['FILE_MONTH']  = df['FILE_DATE'].dt.month
    print(dic_formatos)
    for clave in dic_formatos:
        print(clave)
        valor = dic_formatos[clave]
        if valor[0] in df.columns:
            df[valor[0]] = df[valor[0]].str.replace(",", ".", regex=True)
            df[valor[0]] = df[valor[0]].str.replace("[^0-9-.]", "", regex=True)
    print(df)
    connection,cdn_connection,engine,bbdd_or = mysql_connection()
    try:
        print("CREACION TABLA")
        tabla = Table(f"tb_asignacion_{nombre_tabla}", MetaData(), autoload_with = engine)
        nombre_columnas_nuevas = [c.name for c in tabla.c]
        diffCols = df.columns.difference(nombre_columnas_nuevas)
        listCols = list(diffCols)
        print("AGREGA DIFERENTES")
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
        print("CARGA TABLA TEMPORAL")
        df.to_sql(tmp.name, connection, if_exists='append',index=False)
        connection.execute(text(f"INSERT IGNORE INTO {tabla.name} SELECT * FROM {tmp.name};"))
        tmp.drop(bind = engine,checkfirst=True)
        logging.getLogger("user").info('Insercion Tabla destino DW')
    except Exception as e:
        print(f"ERROR {e}")
        logging.getLogger("user").exception(e)
        raise

def toSqlExcel(path,nombre_tabla,file_, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla):
    print("EN EL TO SQL PARA EXCEL")
    print(dic_formatos)
    logging.info('Lectura archivo')
    print(file_)
    print(dic_hojas)
    for sheet in dic_hojas:
        try:
            print(sheet)
            if sheet == "None":
                df = pd.read_excel(path+"/"+file_[22:],dtype=str)
            else:
                df = pd.read_excel(path+"/"+file_[22:],dtype=str, sheet_name=sheet)
            print(df)
            fecha= datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(path, file_[22:])))
            tabla_reemplazo = str.maketrans({"á":"a","é":"e","í":"i","ó":"o","ú":"u","ñ":"n","Á":"A","É":"E","Í":"I","Ó":"O","Ú":"U","Ñ":"N"})
            df.columns = df.columns.str.strip()
            df.columns = df.columns.str.replace(" ", "_", regex=True)
            df.columns = df.columns.str.translate(tabla_reemplazo)
            df.columns = df.columns.str.replace("[^0-9a-zA-Z_]", "", regex=True)
            df.columns = [insertar_raya_al_piso(nombre_columna) for nombre_columna in df.columns]
            df.columns = df.columns.str.upper()
            print(dic_fechas)
            for clave in dic_fechas:
                valor = dic_fechas[clave]
                print(valor)
                if valor[0] in df.columns:
                    df[valor[0]]=pd.to_datetime(df[valor[0]], format=valor[1], errors='ignore')
                
            df['FILE_DATE'] = fecha
            df['FILE_NAME'] = file_[22:]
            df['FILE_YEAR'] = df['FILE_DATE'].dt.year
            df['FILE_MONTH']  = df['FILE_DATE'].dt.month
            if cargue_tabla == 1:
                print("ES UNA SOLA TABLA")
                df['HOJA_DATA'] = sheet
                print(dic_formatos)
                nombre_tabla1 = f"{nombre_tabla}"
            else:
                if sheet!="None":
                    nombre_tabla1= f"{nombre_tabla}_{sheet}"
            for clave in dic_formatos:
                print(clave)
                valor = dic_formatos[clave]
                if valor[0] in df.columns:
                    df[valor[0]] = df[valor[0]].str.replace(",", ".", regex=True)
                    df[valor[0]] = df[valor[0]].str.replace("[^0-9-.]", "", regex=True)
            print(df)
            connection,cdn_connection,engine,bbdd_or = mysql_connection()
        
        
            print("CREACION TABLA")
            tabla = Table(f"tb_asignacion_{nombre_tabla1}", MetaData(), autoload_with = engine)
            nombre_columnas_nuevas = [c.name for c in tabla.c]
            diffCols = df.columns.difference(nombre_columnas_nuevas)
            listCols = list(diffCols)
            print("AGREGA DIFERENTES")
            if len(listCols)!=0:
                for i in range(len(listCols)):
                    logging.getLogger("user").info(f"Columna {i} agregada.")
                    connection.execute(text(f"ALTER TABLE `{bbdd_or}`.`{tabla.name}` ADD COLUMN `" + listCols[i] + "` VARCHAR(128)"))
            tabla = Table(f"tb_asignacion_{nombre_tabla1}", MetaData(), autoload_with = engine)
            columnas_nuevas = [Column(c.name, c.type) for c in tabla.c]
            tmp = Table(f"{tabla.name}_tmp", MetaData(), *columnas_nuevas)
            tmp.drop(bind = engine,checkfirst=True)
            tmp.create(bind = engine)
            logging.getLogger("user").info('Creacion Tabla temporal')
            # Cargue del DataFrame de Dask en la base de datos MySQL.
            print("CARGA TABLA TEMPORAL")
            df.to_sql(tmp.name, connection, if_exists='append',index=False)
            connection.execute(text(f"INSERT IGNORE INTO {tabla.name} SELECT * FROM {tmp.name};"))
            tmp.drop(bind = engine,checkfirst=True)
            logging.getLogger("user").info('Insercion Tabla destino DW')
        except Exception as e:
            print(f"ERROR {e}")
            logging.getLogger("user").exception(e)



def check_and_add(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla):
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
            toSqlExcel(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla)
        elif extension in [".zip", ".ZIP"]:
            toSqlZip(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador)
        send(f"Se acaba de cargar la base {file[22:]} ")
        logging.getLogger("user").info(f"Base '{file[22:]}' del {file[:20]} recien cargada [ToSQL]")

    except Exception as e:
        with open(LOADED_FILES, "w") as f:
            f.write("\n".join(file_tmp))
        logging.getLogger("user").exception(f"Error en archivo: {file}\n{e}")
        send(f"Error cargando la base {file}: {e}")
    finally:
        fin = time.time()
        logging.getLogger("user").info(f"Tiempo total de ejecucion: {ini-fin} de {file}")

def scan_folder(path,nombre_tabla,nombre_archivo,dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla):
    print(f"en Scan {nombre_tabla} -- {nombre_archivo}-- {path}")
    try:
        if os.path.exists(path):
            file_to_load = Read_files_path(path,nombre_tabla,nombre_archivo)
            print(file_to_load)
            for file in file_to_load:
                print("cargando",file)
                check_and_add(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla )
        else:
            if hora == '08:00':
                send(f"No ha habido cargue de la base {nombre_tabla} del {anio}-{mes}-{dia}")
                print(f"No ha habido cargue de la base {nombre_tabla} del {anio}-{mes}-{dia}")
         
    except Exception as e:
        print(e)