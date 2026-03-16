@data_loader
def load_data_from_api(*args, **kwargs):
    # params obligatorios
    start = kwargs["fecha_inicio"]
    end = kwargs["fecha_fin"]
    page_size = int(kwargs.get("page_size", 200))

    base_url = get_secret_value("BASE_URL")
    token = get_secret_value("API_TOKEN")

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    rows = []
    offset = 0

    while True:
        params = {"start": start, "end": end, "limit": page_size, "offset": offset}
        r = requests.get(base_url, headers=headers, params=params, timeout=30)
        r.raise_for_status()

        items = r.json().get("items", [])

        for it in items:
            rows.append({
                "id": str(it.get("id")),           # clave para upsert
                "payload": json.dumps(it),         # raw
                "extract_window_start_utc": start,
                "extract_window_end_utc": end,
            })

        if len(items) < page_size:
            break

        offset += page_size

    return pd.DataFrame(rows)