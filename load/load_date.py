import traceback
from util.db_connection import Db_Connection
import pandas as pd

def cargar_dates ():
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
        
        sql_stmt = "SELECT * FROM ext_date"
        dates_tra = pd.read_sql (sql_stmt , ses_sta_db)

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
        
        dim_date_dict = {
            "id":[],
            "date_bk": [], 
            "date_month": [],
            "date_year": [],
        }

        if not dates_tra.empty:
            for did,dbk,dmo,dye \
                in zip(dates_tra['date_id'], dates_tra['date'], dates_tra['month'], dates_tra['year']):
                dim_date_dict['id'].append(did)
                dim_date_dict['date_bk'].append(dbk)
                dim_date_dict['date_month'].append(dmo)
                dim_date_dict['date_year'].append(dye)

            if dim_date_dict['id']:
                df_dim_date = pd.DataFrame(dim_date_dict)
                df_dim_date.to_sql('dim_date', ses_sor_db, if_exists='append', index=False)

    except:
        traceback.print_exc()
    finally:
        pass