from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("Spark_Practica_Basica")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# 1) Dataset chiquito en memoria (para practicar sin descargar nada)
data = [
    (1, "2026-03-01 10:00:00", "Quito",  12.50, 1),
    (2, "2026-03-01 11:15:00", "Quito",   8.00, 0),
    (3, "2026-03-01 12:20:00", "Guayaquil", 15.00, 1),
    (4, "2026-03-02 09:10:00", "Quito",  20.00, 1),
    (5, "2026-03-02 18:30:00", "Guayaquil",  5.50, 0),
]
cols = ["order_id", "order_ts", "city", "amount", "is_discounted"]

df = spark.createDataFrame(data, cols)

# 2) Transformaciones (silver): tipar, crear columnas, filtrar
silver = (
    df
    .withColumn("order_ts", F.to_timestamp("order_ts"))
    .withColumn("year_month", F.date_format("order_ts", "yyyy-MM"))
    .withColumn("net_amount", F.when(F.col("is_discounted") == 1, F.col("amount") * 0.9).otherwise(F.col("amount")))
    .filter(F.col("amount") >= 0)
    .select("order_id", "order_ts", "year_month", "city", "amount", "net_amount", "is_discounted")
)

# 3) Agregación (gold): groupBy + métricas
gold = (
    silver
    .groupBy("year_month", "city")
    .agg(
        F.count("*").alias("num_orders"),
        F.sum("net_amount").alias("net_revenue"),
        F.avg("net_amount").alias("avg_ticket")
    )
    .orderBy("year_month", "city")
)

gold.show(truncate=False)

# 4) Guardar resultado (opcional)
gold.write.mode("overwrite").parquet("out/gold_orders_by_month_city")