
## 1) Qué te pueden evaluar de este lab (Spark básico)

### A) Dockerización mínima de Spark en notebook

* **Por qué necesitas Java**: Spark corre sobre JVM (Scala/Java), aunque uses PySpark.
* Dockerfile:

  * `python:3.11-slim`
  * `openjdk-21-jre` (y a veces jdk)
  * `JAVA_HOME` + `PATH`
  * instala requirements
  * expone puertos `8888` (jupyter) y `4040` (Spark UI)
* docker-compose:

  * bind mounts para `./notebooks` y `./data`
  * puertos 8888:8888 y 4040:4040
  * env de pyspark python

📌 Trampa típica:

* Si `JAVA_HOME` está mal, Spark no levanta.
* `4040` solo funciona cuando hay un SparkContext activo; si reinicias sesión puede cambiar a 4041, 4042, etc.

---

### B) Arquitectura básica de Spark (aunque estés en `local[*]`)

Cuando haces:

```python
SparkSession.builder.master("local[*]").getOrCreate()
```

* **Driver**: tu proceso Python (en el contenedor) que crea el plan (DAG) y coordina.
* **Executors**: en modo local, son hilos/procesos en la misma máquina (no cluster real).
* **Lazy evaluation**: transformaciones no corren hasta una acción.
* **DAG + stages + tasks**: Spark divide el trabajo internamente.
* **Shuffles**: operaciones como `groupBy` suelen requerir shuffle (costo alto); por eso configuras `spark.sql.shuffle.partitions`.

---

## 2) Lo que hiciste: traducido a “conceptos examinables”

### A) Ingesta de datos (requests + guardar parquet)

* Similar a la ingesta en pandas: descargas dataset, lo guardas en `/workspace/data`.
* Esto es “raw landing” local.

### B) Leer Parquet en Spark

```python
df = app.read.parquet(str(ubicacion))
df.printSchema()
```

* Parquet: schema embebido, lectura eficiente.

### C) Transformación “silver” (quality + features)

En tu `silver_df` evalúan que sepas:

* `withColumnRenamed`
* `withColumn` con funciones:

  * `to_date`, `date_format`, `hour`, `unix_timestamp`
* filtros de calidad:

  * distancia >= 0, total_amount >= 0, duración >= 0
* cálculo de duración:

```python
(unix_timestamp(dropoff)-unix_timestamp(pickup))/60
```

📌 Trampa típica:

* Si `dropoff_ts < pickup_ts` te da negativo → por eso filtras.

### D) Enriquecimiento con zonas (joins)

* Creas 2 “dimensiones” desde zones para pickup y dropoff:

  * renombrar columnas para evitar colisiones
* Join left:

  * mantienes todos los viajes aunque falte zona

### E) Agregación “gold”

GroupBy por `pickup_month` y `PU_zone`:

* `sum(total_amount)` revenue
* `count(*)` viajes
* `tip_pct` = total_tip / total_fare con manejo de división por cero usando `when/otherwise`.

Esto es básicamente: **silver limpio → gold agregado**.

---

## 3) Spark UI (puerto 4040) qué debes saber decir

En `localhost:4040` puedes ver:

* Jobs, Stages, Tasks
* DAG visualization
* Tiempo en shuffle, lectura, ejecución
* SQL tab (si usas Spark SQL / DataFrame ops)

Pregunta típica:

> ¿Por qué un `groupBy` es costoso?
> Porque suele provocar **shuffle** (redistribución de datos entre particiones).

---

## 4) Conceptos cortos que suelen preguntar

### Transformación vs Acción (LAZY)

* Transformaciones: `select`, `filter`, `withColumn`, `groupBy` (devuelven DataFrame)
* Acciones: `show`, `count`, `collect`, `write.parquet`, `take`

Sin acción, Spark no ejecuta.

### Particiones en Spark (no confundir con particiones SQL)

* Spark partitions = chunks de datos para paralelismo.
* `spark.sql.shuffle.partitions` controla cuántas particiones se usan en shuffles (default suele ser alto).
* Más particiones ≠ siempre mejor (overhead).

---

# 5) Pregunta tipo examen (realista, nivel “intro”)

## 📝 Enunciado

Tienes un contenedor Docker con Jupyter y Spark en modo local.

1. Escribe un `docker-compose.yml` mínimo que exponga:

   * 8888 para Jupyter
   * 4040 para Spark UI
   * bind mounts `./notebooks` y `./data` hacia `/workspace/notebooks` y `/workspace/data`

2. En PySpark, escribe el código para:

   * Crear una `SparkSession` en `local[*]` con `spark.sql.shuffle.partitions = 8`.
   * Descargar `yellow_tripdata_2024-01.parquet` al directorio `data/` (puede ser con `requests`).
   * Leer el parquet en un DataFrame.
   * Crear un DataFrame “silver” que:

     * renombre pickup/dropoff a `pickup_ts`/`dropoff_ts`
     * calcule `pickup_month` (`yyyy-MM`)
     * calcule `trip_duration_min`
     * filtre registros con `trip_distance >= 0` y `total_amount >= 0` y `trip_duration_min >= 0`
   * Agregar (gold) por `pickup_month` y calcular:

     * `total_revenue = sum(total_amount)`
     * `num_trips = count(*)`
   * Mostrar los primeros 10 resultados ordenados por `pickup_month` y `total_revenue desc`.

3. Explica en 3 líneas:

   * Qué es una transformación y qué es una acción en Spark.
   * Por qué `groupBy` puede ser costoso.

---


### (3) Explicación

* Transformación: operación “lazy” que define un nuevo DataFrame (ej. filter, withColumn, groupBy).
* Acción: dispara ejecución real (ej. show, count, collect, write).
* `groupBy` puede ser costoso porque suele requerir **shuffle** (repartir datos entre particiones), lo que implica red/IO y overhead.
