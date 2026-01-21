import pandas as pd
import sqlalchemy
import click
from tqdm.auto import tqdm

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL usuario')
@click.option('--pg-password', default='root', help='PostgreSQL contraseña')
@click.option('--pg-host', default='week_1-warehouse-1', help='Host de PostgreSQL')
@click.option('--pg-port', default='5432', help='Puerto de la PostgreSQL')
@click.option('--pg-db', default='source', help='Nombre de la DB de PostgreSQL')
@click.option('--year', default=2010, help='Año de ingesta de datos')
@click.option('--month', default='01', help='Mes de ingesta de datos')
@click.option('--target-table', default = 'trips_raw', help='Nombre de la tabla destino')
@click.option('--chunksize', default=10000, help='Tamaño del chunk de ingesta')
def main(pg_user, pg_password, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
    prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data'

    url = f'{prefix}/yellow_tripdata_{year}-{month}.parquet'

    engine = sqlalchemy.create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    df = pd.read_parquet(url)

    first_chunk = True
    init = 0
    end = chunksize
    for i in tqdm(range(df.shape[0] // chunksize + 1)): # Dentro del for debe ser los valores como enteros si se divide normal da float
        print(f'Processing chunk {i}')
        if i > 20:
            break
        if first_chunk:
            df.head(0).to_sql(name=target_table, con=engine, if_exists='replace')
            first_chunk = False
        df.iloc[init:end].to_sql(name=target_table, con=engine, if_exists='append', index=False)
        init = end
        end += chunksize

    print(f'Ingested data to table {target_table} in database {pg_db}')

    print('Validation of ingested data:')

    data = pd.read_sql(f'SELECT COUNT(*) FROM {target_table}', con=engine)
    print("Total Ingested Data: ", data)


if __name__=="__main__":
    main()

# Muy importante manejar loggers en aplicaicones externas
# incorporar manejo de errores con sleep y reintentos