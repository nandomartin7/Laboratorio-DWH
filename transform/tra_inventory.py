import traceback
from util.db_connection import Db_Connection
import pandas as pd
from sqlalchemy import text


def transformar_inventorys():
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

        sql_stmt = "SELECT i.inventory_id, i.film_id, i.store_id, i.last_update,\
                    f.rental_rate, f.replacement_cost\
                    FROM staging.ext_inventory AS i\
                    LEFT JOIN staging.ext_film AS f ON i.film_id = f.film_id;"
        inve_tra = pd.read_sql(sql_stmt, ses_db)
        inve_tra['last_update'] = pd.to_datetime(inve_tra['last_update']).dt.strftime('%Y%d%m')

        with ses_db.connect() as connection:
            # Actualizar los film_id en staging.tra_invent con los valores de sor.dim_film
            update_film_query = text("""
                UPDATE staging.tra_invent i 
                JOIN sor.dim_film f ON i.film_id = f.film_bk
                SET i.film_id = f.id
            """)
            connection.execute(update_film_query)

            # Actualizar los store_id en staging.tra_invent con los valores de sor.dim_store
            update_store_query = text("""
                UPDATE staging.tra_invent i
                JOIN sor.dim_store s ON i.store_id = s.store_bk
                SET i.store_id = s.id
            """)
            connection.execute(update_store_query)

    
        return inve_tra
    except:
        traceback.print_exc()
    finally:
        pass
