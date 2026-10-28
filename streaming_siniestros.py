
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, upper, trim, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType
)

# UNAD - BIG DATA - TAREA 3
# Procesamiento Streaming de siniestros viales
# Fuente: Datos Abiertos de Colombia
# Los registros historicos simulan un flujo en tiempo real

spark = (
    SparkSession.builder
    .appName("Streaming_Siniestros_Colombia")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Estructura de los mensajes JSON enviados por Kafka
schema = StructType([
    StructField("marca_vehiculo", StringType()),
    StructField("modelo_vehiculo", StringType()),
    StructField("tipo_vehiculo", StringType()),
    StructField("edad_vehiculo", StringType()),
    StructField("fecha_accidente", StringType()),
    StructField("gravedad_accidente", StringType()),
    StructField("departamento_accidente", StringType()),
    StructField("municipio_accidente", StringType()),
    StructField("autoridad_de_transito", StringType())
])

# Lectura continua del topico Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "siniestros_colombia")
    .option("startingOffsets", "earliest")
    .load()
)

# Convertir los mensajes Kafka a columnas
datos = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"), schema
        ).alias("registro")
    )
    .select("registro.*")
    .filter(col("tipo_vehiculo").isNotNull())
    .withColumn(
        "tipo_vehiculo",
        upper(trim(col("tipo_vehiculo")))
    )
    .withColumn(
        "gravedad_accidente",
        upper(trim(col("gravedad_accidente")))
    )
)

# Estadisticas acumuladas por tipo y gravedad
estadisticas = (
    datos.groupBy(
        "tipo_vehiculo",
        "gravedad_accidente"
    )
    .agg(count("*").alias("total"))
)

print("======================================")
print("STREAMING SINIESTROS VIALES COLOMBIA")
print("Apache Kafka + Spark Structured Streaming")
print("Esperando mensajes del topico...")
print("======================================")

# Mostrar resultados actualizados en consola
consulta = (
    estadisticas.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("numRows", "50")
    .trigger(processingTime="10 seconds")
    .start()
)

try:
    consulta.awaitTermination()
except KeyboardInterrupt:
    print("Deteniendo el procesamiento Streaming...")
finally:
    consulta.stop()
    spark.stop()
