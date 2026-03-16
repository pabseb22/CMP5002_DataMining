import pandas as pd
import sqlalchemy

def main():
    url = "test-url.com"

    chunksize = 10000

    engine = sqlalchemy.create_engine('postgresql://root/root@postgres_container:5432/source')

    df = pd.read_parquet(url)

    first_row = True

    for i in range(df.shape[0]//chunksize +1):
        if first_row:
            df.head(0).to_sql("trips", engine, if_exists="replace")
        
        df.iloc[i:i+chunksize].to_sql(
            "trips",
            engine,
            schema="raw",
            if_exists="append",
            index=False
        )
    #Validation:
    data = pd.read_sql(f'SELECT COUNT(*) FROM trips', con=engine)

#################################################
# Si fuera CSV se debe pasar el Esquema:
#################################################

# dtype = { # Esto es lo que se va enviar a la base de datos
#     'VendorID': 'Int64', # ID del Taxi como tal
#     'passenger_count': 'Int64',
# }

# df_iter = pd.read_csv(url, chunksize=chunksize, dtype=dtype)

# for i, df in enumerate(df_iter):
#     if i == 0:
#         # Crear la tabla la primera vez
#         df.head(0).to_sql(target_table, engine, if_exists="replace", index=False)

#     # Insertar los datos
#     df.to_sql(target_table, engine, if_exists="append", index=False)

#################################################
# CLICK PARA CLI
#################################################

# @click.command()
# @click.option('--pg-user', default='root', help='PostgreSQL usuario')
# @click.option('--pg-password', default='root', help='PostgreSQL contraseña')
# @click.option('--pg-host', default='week_1-warehouse-1', help='Host de PostgreSQL')
# @click.option('--pg-port', default='5432', help='Puerto de la PostgreSQL')
# @click.option('--pg-db', default='source', help='Nombre de la DB de PostgreSQL')
# @click.option('--year', default=2010, help='Año de ingesta de datos')
# @click.option('--month', default='01', help='Mes de ingesta de datos')
# @click.option('--target-table', default = 'trips_raw', help='Nombre de la tabla destino')
# @click.option('--chunksize', default=10000, help='Tamaño del chunk de ingesta')
# def main(pg_user, pg_password, pg_host, pg_port, pg_db, year, month, target_table, chunksize):