@data_exporter
def export_data_to_postgres(df, **kwargs):
    if df is None or df.empty:
        return

    import psycopg2
    from psycopg2.extras import execute_values
    from mage_ai.data_preparation.shared.secrets import get_secret_value

    conn = psycopg2.connect(
        host=get_secret_value("POSTGRES_HOST"),
        port=get_secret_value("POSTGRES_PORT"),
        dbname=get_secret_value("POSTGRES_DB"),
        user=get_secret_value("POSTGRES_USER"),
        password=get_secret_value("POSTGRES_PASSWORD"),
    )

    sql = """
        INSERT INTO raw.api_raw (id, payload, extract_window_start_utc, extract_window_end_utc)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            payload = EXCLUDED.payload,
            extract_window_start_utc = EXCLUDED.extract_window_start_utc,
            extract_window_end_utc = EXCLUDED.extract_window_end_utc,
            ingested_at_utc = now();
    """

    values = [
        (
            r["id"],
            r["payload"],
            r["extract_window_start_utc"],
            r["extract_window_end_utc"],
        )
        for r in df.to_dict("records")
    ]

    with conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)

    conn.close()