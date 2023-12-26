# Importar librerias necesarias
import os
import sys
import pandas as pd
from Functions import *
from Connections import *
sys.path.append(os.path.join(".."))
from Imports import *

#---- Docuento basado en funciones para trabajar archivos excel y csv y comprimidos ----# 
#-- Función inicial scan_folder
#-- Función segudaria Read_files_path
#-- Función terciaria check_and_add
#-- Funciónes de tratamiento y cargue la informacion a las tablas: toSqlTxt,toSqlExcel
#-- Funciónes de tratamiento de datos en columnas: convertir_fecha,insertar_raya_al_piso 

with open("/root/PROCESOS/BASESMOVISTAR/src/log.yml") as f:
    cnf = yaml.safe_load(f)
    logging.config.dictConfig(cnf)

# Variables globales para el proceso
inicio  = time.time()
anio    = datetime.datetime.now().strftime("%Y")
mes     = datetime.datetime.now().strftime("%m")
dia     = datetime.datetime.now().strftime("%d")
hora    = datetime.datetime.now().strftime("%H:%M")
fecha   = datetime.datetime.now().strftime("%Y-%m-%d")

# Función segundaria del archivo
def Read_files_path(path_,nombre_tabla,nombre_archivo):
    try: 
        print("BUSCANDO ARCHIVOS")
        archivos_coincidentes =[]
        print(nombre_archivo)
        archivos_total = os.listdir(path_) # Listar los archivos del direcctorio 
        print(archivos_total)
        for a in nombre_archivo: # Iterar loa archivos del directrio 
            archivos = [valor for valor in archivos_total if f"{a}" in valor] # Selecionar los archivos deseados
            archivos_coincidentes.extend(archivos) # agregar el archivo a la lista 
        print(f"cantidad archivos: {archivos_coincidentes}\n")
    except Exception as e:
        print("error")
        logging.getLogger("user").exception(e)
        raise 
    with open(f"//root//PROCESOS//BASESMOVISTAR//src//LoadedFiles//{nombre_tabla}.log", "r") as f:
        loaded_files = f.read().splitlines()  # todo el listado 
        # Obtener la lista de archivos con fechas de modificación
        archivos_con_fecha = [f"{datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(path_, archivo))).strftime('%Y-%m-%d %H:%M:%S')} - {archivo}" for archivo in archivos_coincidentes]
        # Comparación de los archivos ya cargados y los archivos para cargar así cargar las diferencias
        different_files = set(archivos_con_fecha).difference(loaded_files)   
    if len(different_files) == 0:
        logging.getLogger("user").debug("No hay archivos para cargar")
        print(different_files)
    return different_files # Archivos a cargar

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
                try:
                    fecha=pd.to_datetime(fecha, format="%d/%m/%Y")
                except:
                    try: 
                        fecha=pd.to_datetime(fecha, format="%m-%d-%y")
                    except:
                        try:
                            if len(str(fecha)) <=5 :
                                x = datetime.datetime(1900, 1, 1)
                                fecha = x + datetime.timedelta(days=(int(fecha) - 2))
                        except:
                            pass

    return str(fecha)
            
def toSqlTxt(path,nombre_tabla,file_, dic_fechas, dic_formatos, separador):
    logging.getLogger("user").info(f"cargando archivo de texto")
    logging.info('Lectura archivo')
    df = dd.read_csv(path+"/"+file_[22:],sep = separador,dtype=str, index_col=False)
    fecha= datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(path, file_[22:])))
    tabla_reemplazo = str.maketrans({"á":"a","é":"e","í":"i","ó":"o","ú":"u","ñ":"n","Á":"A","É":"E","Í":"I","Ó":"O","Ú":"U","Ñ":"N"})
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(" ", "_", regex=True)
    df.columns = df.columns.str.translate(tabla_reemplazo)
    df.columns = df.columns.str.replace("[^0-9a-zA-Z_]", "", regex=True)
    df.columns = [insertar_raya_al_piso(nombre_columna) for nombre_columna in df.columns]
    df.columns = df.columns.str.upper()
    print(dic_fechas)
    for fecha_mod in dic_fechas:
        if fecha_mod in df.columns:
            df[fecha_mod] = df[fecha_mod].apply(convertir_fecha, meta=(f"{fecha_mod}", "str"))
           
    df['FILE_DATE'] = fecha
    df['FILE_NAME'] = file_[22:]
    df['FILE_YEAR'] = df['FILE_DATE'].dt.year
    df['FILE_MONTH']  = df['FILE_DATE'].dt.month
    print(dic_formatos)
    for formato in dic_formatos:
        if formato in df.columns:
            df[formato] = df[formato].str.replace(",", ".", regex=True)
            df[formato] = df[formato].str.replace("[^0-9-.]", "", regex=True)
    connection,cdn_connection,engine,bbdd_or = mysql_connection()
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
        dask.config.set(scheduler="processes")
        df.repartition(npartitions=10)
        columnas_nuevas = [Column(c.name, c.type) for c in tabla.c]
        tmp = Table(f"{tabla.name}_tmp", MetaData(), *columnas_nuevas)
        tmp.drop(bind = engine,checkfirst=True)
        tmp.create(bind = engine)
        logging.getLogger("user").info('Creacion Tabla temporal')
        # Cargue del DataFrame de Dask en la base de datos MySQL.
        df.to_sql(tmp.name, connection, if_exists='append',index=False,parallel=True)
        connection.execute(text(f"INSERT IGNORE INTO {tabla.name} SELECT * FROM {tmp.name};"))
        tmp.drop(bind = engine,checkfirst=True)
        logging.getLogger("user").info('Insercion Tabla destino DW')
    except Exception as e:
        print(f"ERROR {e}")
        logging.getLogger("user").exception(e)
        raise

def toSqlExcel(path,nombre_tabla,file_, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion):
    try:
        logging.info('Lectura archivo')
        excel_file = pd.ExcelFile(path+"/"+file_[22:])
        hojas_documento = excel_file.sheet_names
        fechaHoraActual=datetime.datetime.now()
        for sheet_1 in hojas_documento:
            Xlsx2csv(path+"/"+file_[22:], outputencoding="utf-8").convert(os.path.join("TemporalFiles",f"{sheet_1}.csv"), sheetname=sheet_1)
            logging.info(f"se creó el archivo {sheet_1}.csv")
        
        archivos_hojas = os.listdir(os.path.join("TemporalFiles"))
        for sheet in archivos_hojas:
            try:
                df = dd.read_csv(os.path.join("TemporalFiles", sheet), sep=",", dtype=str)
                if "B2B.csv" in hojas_documento and asignacion==1:
                    df["CAMPAÑA"]="B2B"
                elif asignacion==1:
                    df["CAMPAÑA"]="B2C"
                fecha= datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(path, file_[22:])))
                tabla_reemplazo = str.maketrans({"á":"a","é":"e","í":"i","ó":"o","ú":"u","ñ":"n","Á":"A","É":"E","Í":"I","Ó":"O","Ú":"U","Ñ":"N"})
                df.columns = df.columns.str.strip()
                df.columns = df.columns.str.replace(" ", "_", regex=True)
                df.columns = df.columns.str.translate(tabla_reemplazo)
                df.columns = df.columns.str.replace("[^0-9a-zA-Z_]", "", regex=True)
                df.columns = [insertar_raya_al_piso(nombre_columna) for nombre_columna in df.columns]
                df.columns = df.columns.str.upper()
                for fecha_mod in dic_fechas:
                    if fecha_mod in df.columns:
                        df[fecha_mod]=df[fecha_mod].apply(convertir_fecha, meta=(f"{fecha_mod}", "str"))
                df['FILE_DATE'] = fecha
                df['FILE_NAME'] = file_[22:]
                df['FILE_YEAR'] = df['FILE_DATE'].dt.year
                df['FILE_MONTH']  = df['FILE_DATE'].dt.month
                df['FECHA_CARGUE'] = fechaHoraActual
                if cargue_tabla == 1:
                    df['HOJA_DATA'] = sheet[:-4]
                    nombre_tabla1 = f"{nombre_tabla}"
                else:
                    if len(archivos_hojas)>=1:
                        nombre_tabla1= f"{nombre_tabla}_{sheet}"
                for clave in dic_formatos:
                    valor = dic_formatos[clave]
                    if valor[0] in df.columns:
                        df[valor[0]] = df[valor[0]].str.replace(",", ".", regex=True)
                        df[valor[0]] = df[valor[0]].str.replace("[^0-9-.]", "", regex=True)
                connection,cdn_connection,engine,bbdd_or = mysql_connection()
                logging.getLogger("user").info('Creacion Tabla temporal')
                tabla = Table(f"tb_asignacion_{nombre_tabla1}", MetaData(), autoload_with = engine)
                nombre_columnas_nuevas = [c.name for c in tabla.c]
                diffCols = df.columns.difference(nombre_columnas_nuevas)
                listCols = list(diffCols)
                if len(listCols)!=0:
                    for i in range(len(listCols)):
                        logging.getLogger("user").info(f"Columna {i} agregada.")
                        connection.execute(text(f"ALTER TABLE `{bbdd_or}`.`{tabla.name}` ADD COLUMN `" + listCols[i] + "` VARCHAR(128)"))
                tabla = Table(f"tb_asignacion_{nombre_tabla1}", MetaData(), autoload_with = engine)
                columnas_nuevas = [Column(c.name, c.type) for c in tabla.c]
                tmp = Table(f"{tabla.name}_tmp", MetaData(), *columnas_nuevas)
                tmp.drop(bind = engine,checkfirst=True)
                tmp.create(bind = engine)
                dask.config.set(scheduler="processes")
                df.repartition(npartitions=10)
                # Cargue del DataFrame de Dask en la base de datos MySQL.
                df.to_sql(tmp.name, cdn_connection, if_exists='append',index=False,parallel=True)
                connection.execute(text(f"INSERT IGNORE INTO {tabla.name} SELECT * FROM {tmp.name};"))
                tmp.drop(bind = engine,checkfirst=True)
                logging.getLogger("user").info('Insercion Tabla destino DW')
            except Exception as e:
                print(f"ERROR {e}")
                logging.getLogger("user").exception(e)
                raise
    except Exception as e:
        print(f"ERROR {e}")
        logging.getLogger("user").exception(e)
        raise
    finally:
        for file in os.listdir(os.path.join("TemporalFiles")):
            os.remove(os.path.join("TemporalFiles", file))

def toSqlZip(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, nombre_archivo, cargue_tabla, asignacion, logs):
    try:
        archivo_zip = path+"/"+file[22:]
        logging.getLogger("user").info(f"El archivo a cargar es {archivo_zip}")
        with zipfile.ZipFile(archivo_zip, 'r') as zip_ref:
            # Extrae todo el contenido en el directorio de destino
            zip_ref.extractall(r"/root/PROCESOS/BASESMOVISTAR/src/ZIP/")
        file_to_load = Read_files_path(os.path.join("ZIP"),nombre_tabla,nombre_archivo)
        for file_1 in file_to_load:
            nombre, extension = os.path.splitext(file_1)
            if extension in [".txt", ".csv"]:
                logging.getLogger("user").info(f"txt del zip")
                toSqlTxt(path,nombre_tabla,file, dic_fechas,dic_formatos,separador)
            elif extension in [".xlsx"]:
                logging.getLogger("user").info(f"excel del zip")
                toSqlExcel(os.path.join("ZIP"),nombre_tabla,file_1, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion)
    except:
        raise
    finally:
        for file in os.listdir(os.path.join("ZIP")):
            os.remove(os.path.join("ZIP", file))

# Función terciaria del archivo
def check_and_add(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion, nombre_archivo):
    logging.getLogger("user").info(f" archivo a cargar{file}")
    ini = time.time() # timepo de inicio del cargue 
    LOADED_FILES = f"//root//PROCESOS//BASESMOVISTAR//src//LoadedFiles//{nombre_tabla}.log" # ruta del log

    with open(LOADED_FILES, "r") as f: # abrir el archivo del log
        logs     = f.read().splitlines()
        file_tmp = f.read().splitlines()
    
    logging.getLogger("user").info(f"La base '{file[22:]}' del {file[:20]} esta por cargarse") # log cargue
    logs.append(file) # Log archivo
    with open(LOADED_FILES, "w") as f:
            f.write(f"\n".join(logs))
    try:
        nombre, extension = os.path.splitext(file) # Obtener el nombre archivo y extención
        print(extension)
        # validacion de extenciones para saber que función usar 
        if extension in [".txt", ".csv"]:
            logging.getLogger("user").info(f"el archivo es un txt")
            toSqlTxt(path,nombre_tabla,file, dic_fechas,dic_formatos,separador)
        elif extension in [".xlsx"]:
            logging.getLogger("user").info(f"el archivo es un excel")
            toSqlExcel(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion)
        elif extension in [".zip", ".ZIP"]:
            logging.getLogger("user").info(f"el archivo es un zip")
            toSqlZip(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, nombre_archivo, cargue_tabla,asignacion, logs)
        send(f"Se acaba de cargar la base {file[22:]} ")
        logging.getLogger("user").info(f"Base '{file[22:]}' del {file[:20]} recien cargada [ToSQL]")

    except Exception as e:
        with open(LOADED_FILES, "w") as f:
            f.write("\n".join(file_tmp))
        logging.getLogger("user").exception(f"Error en archivo: {file}\n{e}")
        send(f"Error cargando la base {file}: {e}")
    finally:
        fin = time.time()
        print(f"TOTAL TIEMPO EJECUCION {fin-inicio}")
        logging.getLogger("user").info(f"Tiempo total de ejecucion: {fin-ini} de {file}")

# Función primaria del archivo 
def scan_folder(path,nombre_tabla,nombre_archivo,dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion):
    print(f"en Scan {nombre_tabla} -- {nombre_archivo}-- {path}")
    try:
        if os.path.exists(path): # validar si la ruta existe
            file_to_load = Read_files_path(path,nombre_tabla,nombre_archivo) # Obtener listado de los archivos a cargar 
            print(file_to_load)
            for file in file_to_load: # interar los archivos para el cargue
                print("cargando",file)
                # validacion y extandarización para el cargue de los archivos 
                check_and_add(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion, nombre_archivo )   
        else:
            if hora == '08:00':
                # mesaje en caso de que no se encuentra la ruta deseada
                send(f"No ha habido cargue de la base {nombre_tabla} del {anio}-{mes}-{dia}") 
         
    except Exception as e:
        print(e)