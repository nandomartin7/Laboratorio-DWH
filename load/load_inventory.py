import traceback
from util.db_connection import Db_Connection
import pandas as pd

def cargar_fact_inventory ():
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'udladwh'
        pwd = 'RealMadrid-7'
        db = 'staging'

        con_sta_db = Db_Connection(type,host,port,user,pwd,db)
        ses_sta_db = con_sta_db.start()
        if ses_sta_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sta_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stmt = "SELECT store_id, film_id, last_update, rental_rate, replacement_cost FROM tra_invent"
        invets_tra = pd.read_sql (sql_stmt , ses_sta_db)

        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'udladwh'
        pwd = 'RealMadrid-7'
        db = 'sor'

        con_sor_db = Db_Connection(type,host,port,user,pwd,db)
        ses_sor_db = con_sor_db.start()
        if ses_sor_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sor_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        dim_FactIn_dict = {
            "store_id": [],
            "film_id": [],
            "date_id": [],
            "rental_price": [],
            "rental_cost": [],
        }

        if not invets_tra.empty:
            for sti,fii,dti,rp,rc\
            in zip(invets_tra['store_id'], invets_tra['film_id'], invets_tra['last_update'], invets_tra['rental_rate'], invets_tra['replacement_cost']):
                dim_FactIn_dict['store_id'].append(sti)
                dim_FactIn_dict['film_id'].append(fii)
                dim_FactIn_dict['date_id'].append(dti)
                dim_FactIn_dict['rental_price'].append(rp)
                dim_FactIn_dict['rental_cost'].append(rc)
                
            
            if dim_FactIn_dict['store_id']:
                df_dim_film = pd.DataFrame(dim_FactIn_dict)
                df_dim_film.to_sql('fact_inventory', ses_sor_db, if_exists='append', index=False)
    except:
        traceback.print_exc()
    finally:
        pass