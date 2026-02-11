import pandas as pd
# Visualizador para ingesta de datos
from tqdm.auto import tqdm
# ORM Permite manejar estructuras de dabses de datos desde condigo
import sqlalchemy
import click

# Diccionario de las columnas. Necesitamos definir esto apra compara como ahcer con CSV.
dtype = { # Esto es lo que se va enviar a la base de datos
    'VendorID': 'Int64', # ID del Taxi como tal
    'passenger_count': 'Int64',
    'trip_distance': 'float64',
    'RatecodeId':'Int64', # Taximetro
    'store_and_fwd_flag': 'Int64',
    'PULocationID':'Int64', #Pickup Location ID
    'DOLocationID':'Int64',
    'payment_type': 'Int64',
    'fare_amount': 'Float64'
}

# Decoradores investigar:
@click.command() #Prtmitr ptogarama de linea de comnados
@click.option('--pg-user', default='root', help='PostgreSQL usuario') #ls es un programa hecho en c. Queremos un python que reciba parametros como -l en "ls -l"
@click.option('--pg-password', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL nombre db')
@click.option('--year', default=2020, help='Year of Data ingest')
@click.option('--month', default=1, help='Month of Data ingest')
@click.option('--target-table', help='Name of target Table')
@click.option('--chunksize', default=100000, help='Chunksize of data to ingest') #to avoind inserting millons of data
def main(pg_user, pg_password, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
    #Vamos a minar datos de ny taxi ny newyork https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page es big data.
    # Problema CSV no tiene el esquema de datos. Es optimziado PARQUET para bigdata y tiene formato columnar. CSV esta por filas.
    # Big Data son formatos columnares se guarda toda la columna en el mismo espacio de memoria y se tiene algoritmos de compresion como PARQUET.
    # PARQUET tiene el esquema dentro, metadata del esquema de la etsructura de datos.
    
    #Estamos construyendo un programa de ilineas de comando
    # prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'

    prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data' #Fuente oficial de datos

    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.parquet' #0>2 es para que tenga 2 digitos ejm 01,02

    # Crear el motor de la base de datos #Revisar forma odbc y jdbc
    engine = sqlalchemy.create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    print('Ingesting data into Postgres')
    df = pd.read_parquet(url)
    df.to_sql(name=target_table, con=engine, if_exists='replace') #Si existe la tabla la reemplaza. Permite indempotencia.
    print(f'Ingested data to table {target_table} in database {pg_db}')

    # Leer el CSV en chunks
    # df_iter = pd.read_csv(url, iterator=True, chunksize=chunksize, dtype=dtype)

    # first_chunk = True

    # for df in tqdm(df_iter, desc="Ingesting data..."):
    #     if first_chunk:
    #         # Crear la tabla en la base de datos
    #         df.head(0).to_sql(name=target_table, con=engine, if_exists='replace') #Si existe la tabla la reemplaza. Permite indempotencia.
    #         first_chunk = False
    #     # Insertar los datos en la tabla
    #     df.to_sql(name=target_table, con=engine, if_exists='append') #Si existe la tabla agrega los datos


if __name__=="__main__":
    main()