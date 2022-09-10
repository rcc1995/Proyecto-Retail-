
##CONSUMO LAS LIBRERIAS
import glob  
import pandas as pd  
import xml.etree.ElementTree as ET  
from datetime import datetime
import sqlalchemy as db
import mysql.connector
import pandas as pd
import MySQLdb
import warnings
warnings.filterwarnings("ignore")


##NOS CONECTAMOS A LA BD MYSQL
engine = db.create_engine("mysql://root:admin@localhost/retail")
conn = engine.connect()



##CREAMOS LA FUNCION PARA LEER EL CSV
def extract_from_csv(file_to_process): 
    dataframe = pd.read_csv(file_to_process, delimiter='|',encoding='ISO-8859-1',header=None) 
    return dataframe

##SETEAMOS LA RUTA GENERAL DE LOS INPUTS
ruta_general='./data/'


#######################################
##INSERTAMOS LOS INPUTS A LAS TABLAS MYSQL
######################################

##PARA DEPARTAMENTOS
archivo='departments'
ruta=ruta_general+archivo

df=extract_from_csv(ruta)
df.columns=['department_id','department_name']

df.to_sql('departments',conn,index = False,if_exists='replace')

##PARA CATEGORIES
archivo='categories'
ruta=ruta_general+archivo

df=extract_from_csv(ruta)
df.columns=['category_id','category_department_id','category_name']

df.to_sql('categories',conn,index = False,if_exists='replace')


##PARA CATEGORIES
archivo='customer'
ruta=ruta_general+archivo

df=extract_from_csv(ruta)
df.columns=['customer_id','customer_fname','customer_lname','customer_email','customer_password','customer_street','customer_city','customer_state','customer_zipcode']

df.to_sql('customers',conn,index = False,if_exists='replace')


##PARA ORDER_ITEMS
archivo='order_items'
ruta=ruta_general+archivo

df=extract_from_csv(ruta)
df.columns=['order_item_id','order_item_order_id','order_item_product_id','order_item_quantity','order_item_subtotal','order_item_product_price']

df.to_sql('order_items',conn,index = False,if_exists='replace')


##PARA ORDERS
archivo='orders'
ruta=ruta_general+archivo

df=extract_from_csv(ruta)
df.columns=['order_id','order_date','order_customer_id','order_status']

df.to_sql('orders',conn,index = False,if_exists='replace')


##PARA PRODUCTS
archivo='products'
ruta=ruta_general+archivo

df=extract_from_csv(ruta)
df.columns=['product_id','product_category_id','product_name','product_description','product_price','product_image']

df.to_sql('products',conn,index = False,if_exists='replace')


