from pyspark.sql import SparkSession
s = SparkSession.builder.appName("Prueba_Kafka").getOrCreate()
d = s.read.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","siniestros_colombia").option("startingOffsets","earliest").option("endingOffsets","latest").load()
print("MENSAJES LEIDOS POR SPARK:", d.count())
s.stop()
