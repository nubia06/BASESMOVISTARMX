import os 
import sys
sys.path.append(os.path.join(".."))
from Imports import *
ip="60"
# -1002010546154 -- chat_id produción
ruta_config_files = os.path.join( "config", f"config_{ip}.json")
with open(ruta_config_files) as f:
    configuracion = json.load(f)
credecial=configuracion["credenciales_bot_tlgrm"]
key = credecial["accesos"]

async def send_message(msg,chat_id): 
        bot = telegram.Bot(key[0]) 
        await bot.send_message(text = msg, chat_id = chat_id)

def send(msg):
    asyncio.run(send_message(msg,key[1]))