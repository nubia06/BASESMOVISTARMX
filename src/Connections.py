from Imports import *
from Credential import *

def mysql_connection(ip,bbdd):
    
    url       = f'mysql+pymysql://{user_mysql}:{quote(password_mysql)}@{ip}:{"3306"}/{bbdd}'
    engine    = create_engine(url,pool_recycle=9600,isolation_level="AUTOCOMMIT")
    mysql_con = engine.connect()
    engine.dispose()
    return mysql_con,url

def mysql_engine(ip,bbdd):
    
    url       = f'mysql+pymysql://{user_mysql}:{quote(password_mysql)}@{ip}:{"3306"}/{bbdd}'
    engine    = create_engine(url,pool_recycle=9600,isolation_level="AUTOCOMMIT")
    return engine

