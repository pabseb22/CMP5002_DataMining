-- Enunciado
-- Tienes un pipeline en Mage que carga:

-- raw.taxi_trips (NYC Yellow trips)
-- raw.zones (taxi_zone_lookup.csv)

-- Debes construir en dbt (y dejarlo organizado por capas):

-- Un modelo bronze stg_taxi_trips tipando columnas y calculando trip_duration_minutes.
-- Un modelo bronze stg_zones renombrando _zone y tipando location_id.
-- Un modelo silver int_taxi_trips_zones_join que:
    -- haga join para obtener pickup_location_name y dropoff_location_name
    -- filtre viajes con passengers_count >= 1

-- Define 3 tests dbt para el modelo silver:

-- pickup_ts not_null
-- pickup_location relationships -> stg_zones.location_id
-- passengers_count accepted_values [0..8]

-- Explica en 4 líneas cómo Mage y dbt se complementan en este pipeline.

-- models/bronze/stg_taxi_trips.sql
with base as (
  select * from {{ source('origen', 'taxi_trips') }}
)
select
  vendorid::int as vendor_id,
  tpep_pickup_datetime::timestamp as pickup_ts,
  tpep_dropoff_datetime::timestamp as dropoff_ts,
  tip_amount::numeric(10,2) as tip_amount,
  tolls_amount::numeric(10,2) as tolls_amount,
  total_amount::numeric(10,2) as total_amount,
  extract(epoch from (tpep_dropoff_datetime::timestamp - tpep_pickup_datetime::timestamp)) / 60.0
    as trip_duration_minutes
from base

--models/bronze/stg_zones.sql
with base as (
  select * from {{ source('origen', 'zones') }}
)
select
  locationid::int as location_id,
  borough,
  _zone as zone_name,
  service_zone
from base

-- models/silver/int_taxi_trips_zones_join.sql
with trips as (
  select * from {{ ref('stg_taxi_trips') }}
),
zones as (
  select * from {{ ref('stg_zones') }}
)
select
  trips.*,
  zp.zone_name as pickup_location_name,
  zd.zone_name as dropoff_location_name
from trips
inner join zones zp on trips.pickup_location = zp.location_id
inner join zones zd on trips.dropoff_location = zd.location_id
where coalesce(trips.passengers_count, 0) >= 1

-- models/silver/schema.yml
version: 2

models:
  - name: int_taxi_trips_zones_join
    columns:
      - name: pickup_ts
        tests: [not_null]

      - name: pickup_location
        tests:
          - not_null
          - relationships:
              to: ref('stg_zones')
              field: location_id

      - name: passengers_count
        tests:
          - not_null
          - accepted_values:
              values: [0,1,2,3,4,5,6,7,8]


-- models/gold/fct_taxi_daily_zone.sql
{{ config(
    materialized='incremental',
    unique_key=['pickup_date','pickup_location'],
    partition_by={'field': 'pickup_date', 'data_type': 'date'},
    incremental_strategy='merge'
) }}

with base as (
  select
    date_trunc('day', pickup_ts)::date as pickup_date,
    pickup_location,
    pickup_location_name,
    count(*) as trips,
    avg(trip_duration_minutes) as avg_trip_duration_minutes,
    sum(total_amount) as total_amount
  from {{ ref('int_taxi_trips_zones_join') }}
  {% if is_incremental() %}
    where pickup_ts >= (select max(pickup_date) from {{ this }})
  {% endif %}
  group by 1,2,3
)

select * from base
