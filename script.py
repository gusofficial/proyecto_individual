import pandas as pd                     
import unidecode                        
import numpy as np                      
import os                               
from sqlalchemy import create_engine    

# Conexión a mysql
my_conn = create_engine("mysql://root:@localhost/db_pi")

#¿ Extracción

archivo = 'precios_semana_20200426.csv'
path = 'datasets/'

# cargamos el archivo a un dataframe
extension = archivo.split('.')[1]
filepath = os.path.join(path,archivo)

if extension == 'csv':
    df = pd.read_csv(filepath)
elif extension == 'txt':
    df = pd.read_csv(filepath, sep = '|')
elif extension == 'json':
    df = pd.read_json(filepath)
elif extension == 'xlsx':
    df = pd.read_excel(filepath)
elif extension == 'parquet':
    df = pd.read_parquet(filepath)  

print('Los datos fueron cargado correctamente.')

# Transformacion

# Eliminamos las columnas que tengan nulos en sucursal_id y producto_id
index_nulls = df.loc[(df.producto_id.isnull()) | (df.sucursal_id.isnull())].index.to_list()
df.drop(index_nulls, axis='index', inplace=True)

# Agregamos id de semana
semana = archivo.split('_')[2].split('.')[0]

df['semanaId'] = semana

# Formateamos el producto id 
if df.producto_id.astype(str).str.contains('.', na=True).sum() > 0:
    df.producto_id = df.producto_id.astype(str).str.replace('.0', '', regex=False)
    df.producto_id = df.producto_id.str.zfill(13)

# Formateamos el sucursal id
if df.sucursal_id.str.contains('00:00:00', na=True).sum() > 0:
    df.loc[df.sucursal_id.str.contains('00:00:00', na=True), 'sucursal_id'] = 'Sin dato'

# Ultimo id en producto
query = "SELECT precioId FROM precios ORDER BY productoId DESC LIMIT 1"
last_id = pd.read_sql(query,my_conn).iloc[0,0]

# Id de precio
df.insert(0, 'precioId', range(last_id+1, last_id +1+ len(df)))

# Faltantes
df_aux = df.loc[(df.precio.isnull()) | (df.precio=='')].copy()
df_aux['tipoError'] = 0

df_aux = pd.concat([df_aux, df.loc[df.productoId.isnull()].copy()])
df_aux.loc[df_aux.productoId.isnull(), 'tipoError'] = 1

# Los dropeamos
index = df.loc[(df.precio.isnull()) | (df.productoId.isnull()) | (df.precio=='')].index
df.drop(index, axis='index', inplace=True)


df.rename({'sucursal_id':'sucursalId'}, axis='columns', inplace=True)
df.productoId = df.productoId.astype(int)
df.precio = df.precio.astype(float)
df_aux.rename({'sucursal_id':'sucursalId'}, axis='columns', inplace=True)
df_aux.tipoError = df_aux.tipoError.astype(int)

df = df[['precioId','precio','productoId','sucursalId','semanaId']]

print('Los datos fueron normalizados.')

# Carga

# Cargo los precios a la tabla precios del db
df.to_sql(con=my_conn, name='precios', if_exists='append', index=False)

# Cargo los datos a la tabla aux del db
df_aux.to_sql(con=my_conn, name='precios_auxiliar', if_exists='append', index=False)

print('Los datos fueron agregados a la base de datos.')