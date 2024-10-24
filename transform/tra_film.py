import traceback
from util.db_connection import Db_Connection
import pandas as pd

def transformar_films():
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
        sql_stmt = "SELECT film_id, title, release_year, length, rating  FROM staging.ext_film;"
        films_tra = pd.read_sql(sql_stmt, ses_db)
        films_tra['duration'] = films_tra['length'].apply(
            lambda x: '< 1h' if x < 60 else
                      '< 1.5h' if x < 90 else
                      '< 2h' if x < 120 else '> 2h'
        )
        return films_tra
    except:
        traceback.print_exc()
    finally:
        pass
