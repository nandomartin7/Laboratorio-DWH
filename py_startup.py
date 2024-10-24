# this file is a kind of python startup module used for manual unit testing

#from util.db_connection import Db_Connection
from extract.ext_countries import extraer_countries
from extract.ext_stores import extraer_stores
from extract.per_staging import persistir_staging
from transform.tra_stores import transformar_stores
from load.load_stores import cargar_stores
from extract.ext_address import extraer_address
from extract.ext_city import extraer_city
from extract.ext_date import extraer_date
from extract.ext_film import extraer_film
from extract.ext_inventory import extraer_inventory
from transform.tra_film import transformar_films
from extract.per_staging_address import persistir_staging_address
from load.load_films import cargar_films
from transform.tra_date import transformar_dates
from load.load_date import cargar_dates
from transform.tra_inventory import transformar_inventorys
from load.load_inventory import cargar_fact_inventory

import traceback
import pandas as pd

try:
    ##TRABAJO SESION CON EL PROFESOR

    #Country
    print("extrayendo datos de countries desde un csv")
    countries = extraer_countries()
    print("Persistiendo en el staging de countries")
    persistir_staging(countries, 'ext_country' )

    #Store
    print("extrayendo datos de stores desde un OLTP")
    stores = extraer_stores()
    print("Persistiendo en el staging de stores")
    persistir_staging(stores, 'ext_store' )
    
    print("transformacion datos de stores en el STAGING")
    tra_stores = transformar_stores()

    print("Persistiendo en el staging datos transfromados de stores")
    persistir_staging(tra_stores, 'tra_store' )
    print("Cargando datos de store en SOR")
    cargar_stores()

    ##Trabajo Postsesion

    #Address
    print("extrayendo datos de addresses desde un OLTP")
    address = extraer_address()
    print("Persistiendo en el staging de address")
    persistir_staging_address(address)
    
    #City
    print("extrayendo datos de city desde un OLTP")
    cities= extraer_city()
    print("Persistiendo en el staging de city")
    persistir_staging(cities, 'ext_city' )


    #Date
    print("Extrayendo datos de date desde un CSV")
    dates = extraer_date()
    print("Persistiendo en el staging de date")
    persistir_staging(dates, 'ext_date' )
    print("transformacion datos de dates en el STAGING")
    tra_dates = transformar_dates()
    print(tra_dates)
    print("Persistiendo en el staging datos transformados de date")
    persistir_staging(tra_dates, 'tra_date')
    print("Cargando datos de date en SOR")
    cargar_dates()


    #Film
    print("Extrayendo datos de film desde un OLTP")
    films = extraer_film()
    print("Persistiendo en el staging de film")
    persistir_staging(films, 'ext_film' )
    print("transformacion datos de films en el STAGING")
    tra_films = transformar_films()
    print("Persistiendo en el staging datos transfromados de film")
    persistir_staging(tra_films, 'tra_film' )
    print("Cargando datos de film en SOR")
    cargar_films()

    #Inventory
    print("Extrayendo datos de inventory desde un OLTP")
    invetories = extraer_inventory()
    print("Persistiendo en el staging de inventory")
    persistir_staging(invetories, 'ext_inventory' )
    print("transformacion datos de inventorys en el STAGING")
    tra_invent = transformar_inventorys()
    print("Persistiendo en el staging datos transfromados de inventory")
    persistir_staging(tra_invent, 'tra_invent' )
    print("Cargando datos de fact_inventory en SOR")
    cargar_fact_inventory()

except:
    traceback.print_exc()
finally:
    pass
    #ses_db_oltp = con_db.stop()