import traceback
from util.db_connection import Db_Connection
import pandas as pd

def transformar_dates():
    try:
        # Configuración de la conexión
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'udladwh'
        pwd = 'RealMadrid-7'
        db = 'staging'

        # Crear la conexión a la base de datos
        con_db = Db_Connection(type, host, port, user, pwd, db)
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")

        # Consulta para extraer los datos de la tabla ext_film
        sql_stmt = "SELECT date_id, date, month, year FROM staging.ext_date;"
        dates_tra = pd.read_sql(sql_stmt, ses_db)
        
        dates_tra['date'] = pd.to_datetime(dates_tra['date']).dt.date
        return dates_tra
    except:
        traceback.print_exc()
    finally:
        pass
