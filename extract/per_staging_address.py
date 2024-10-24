import traceback
from util.db_connection import Db_Connection
import pandas as pd

def persistir_staging_address(df_stg):
    try:
        # Parámetros para la conexión a la base de datos staging
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
            raise Exception("Error tratando de conectarse a la base de datos staging")

        # Verificar si la columna 'location' está en el DataFrame y eliminarla
        if 'location' in df_stg.columns:
            df_stg = df_stg.drop(columns=['location'])

        # Persistir los datos en la tabla ext_address
        df_stg.to_sql('ext_address', ses_db, if_exists='replace', index=False)
        print("Datos persistidos correctamente en staging.ext_address")

    except:
        traceback.print_exc()
    finally:
        pass
