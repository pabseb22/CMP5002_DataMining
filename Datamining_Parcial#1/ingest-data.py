import sqlalchemy
import pandas as pd
from tqdm.auto import tqdm
import click
import math

dtype = {
    'VendorID': 'Int64',
    'passenger_count': 'Int64',
    'trip_distance': 'float64',
    'RatecodeID': 'Int64',
    'store_and_fwd_flag': 'Int64',
    'PULocationID': 'Int64',
    'DOLocationID': 'Int64',
    'payment_type': 'Int64',
    'fare_amount': 'float64'
}

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL usuario')
@click.option('--pg-password', default='root', help='PostgreSQL contraseña')
@click.option('--pg-host', default='semana-1-4-warehouse-1', help='Host de PostgreSQL')
@click.option('--pg-port', default='5432', help='Puerto de la PostgreSQL')
@click.option('--pg-db', default='source', help='Nombre de la DB de PostgreSQL')
@click.option('--year',default=2010, help='Año de ingesta de datos')
@click.option('--month', default='01', help='Mes de ingesta de datos')
@click.option('--target-table', default = 'trips_raw', help='Nombre de la tabla destino')
@click.option('--chuncksize', default=100000, help='Tamaño del chunk de ingesta')
def ingest(pg_user, pg_password, pg_host, pg_port,pg_db, year, month, target_table, chuncksize):

    print('Inicio de ingesta...')
    url_parquet = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet'

    print('Creacion del motor de conexion al WH...')
    motor = sqlalchemy.create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    print('Inicio de descarga de datos...')
    datos_crudos = pd.read_parquet(
        url_parquet,
    )

    print(f'Datos descargados con forma: {datos_crudos.shape}')

    first = True
    init = 0
    end = chuncksize
    max_reintentos = 5

    print(f'Inicio de carga al WH de los datos...')
    for i in tqdm(range(1, math.ceil(datos_crudos.shape[0]/chuncksize))):
        
        if first:
            datos_crudos.head(0).to_sql(
                name=target_table,
                con=motor,
                if_exists='replace'
            )
            first = False

        for reintentos in range(max_reintentos):
            try:
                datos_crudos.iloc[init:end].to_sql(
                    name=target_table,
                    con=motor,
                    if_exists='append'
                )
                # sleep(2 ** reintentos) -> crece
            except:
                pass

        init = end
        end = i * chuncksize

    print('Los datos fueron ingestados...')
    print('Iniciando validacion de datos...')

    datos_wh = pd.read_sql(
        f"""
        select count(*) from {target_table}
        """,
        con=motor
    )

    print(f"Datos ingestados: {datos_wh.iloc[0,0]}")

if __name__ == "__main__":
    ingest()
