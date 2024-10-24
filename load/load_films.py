import traceback
from util.db_connection import Db_Connection
import pandas as pd

def cargar_films ():
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
        
        sql_stmt = "SELECT * FROM tra_film"
        films_tra = pd.read_sql (sql_stmt , ses_sta_db)

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
        
        dim_film_dict = {
            "film_bk": [],
            "title": [],
            "release_year": [],
            "length": [],
            "duration": [],
            "rating": [],
        }

        if not films_tra.empty:
            for fbk,tit,rel,leng,dur,rati \
            in zip(films_tra['film_id'], films_tra['title'], films_tra['release_year'], films_tra['length'], films_tra['duration'], films_tra['rating']):
                dim_film_dict['film_bk'].append(fbk)
                dim_film_dict['title'].append(tit)
                dim_film_dict['release_year'].append(rel)
                dim_film_dict['length'].append(leng)
                dim_film_dict['duration'].append(dur)
                dim_film_dict['rating'].append(rati)
            
            if dim_film_dict['film_bk']:
                df_dim_film = pd.DataFrame(dim_film_dict)
                df_dim_film.to_sql('dim_film', ses_sor_db, if_exists='append', index=False)

    except:
        traceback.print_exc()
    finally:
        pass