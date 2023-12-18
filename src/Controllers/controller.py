# Importar librerias necesarias
import os
import sys
sys.path.append(os.path.join(".."))
from Imports import *

# Variables globales para el proceso
inicio  = time.time()
anio    = datetime.datetime.now().strftime("%Y")
mes     = datetime.datetime.now().strftime("%m")
dia     = datetime.datetime.now().strftime("%d")
hora    = datetime.datetime.now().strftime("%H:%M")
fecha   = datetime.datetime.now().strftime("%Y-%m-%d")
