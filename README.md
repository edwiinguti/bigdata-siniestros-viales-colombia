# Análisis de siniestros viales en Colombia — Big Data

**Universidad Nacional Abierta y a Distancia (UNAD)**

**Curso:** Big Data  
**Actividad:** Tarea 3 — Procesamiento de Datos con Apache Spark  
**Estudiante:** Edwin Andrés Gutiérrez Agudelo

## 1. Descripción del proyecto

Este proyecto tiene como propósito analizar una base de datos de siniestros viales de Colombia, compuesta por 450.335 registros de vehículos involucrados en accidentes de tránsito.

Para su desarrollo se implementaron dos metodologías de procesamiento de datos:

- **Procesamiento batch:** análisis de información histórica mediante Apache Spark.
- **Procesamiento Streaming:** simulación de transmisión y procesamiento continuo de registros mediante Apache Kafka y Spark Structured Streaming.

## 2. Herramientas utilizadas

- Python
- Apache Spark y PySpark
- Apache Kafka
- Spark Structured Streaming
- Ubuntu
- Oracle VirtualBox
- GitHub

## 3. Procesamiento batch

Se desarrolló el programa `analisis_siniestros.py`, encargado de cargar, revisar y analizar la información histórica.

El procesamiento permitió generar estadísticas y gráficos relacionados con la ubicación geográfica, el tipo de vehículo, la gravedad del accidente y el año del registro.

### Principales resultados

| Ubicación | Registros |
|---|---:|
| Colombia | 450.335 |
| Valle del Cauca | 56.503 |
| Palmira | 5.968 |

A nivel nacional, las motocicletas presentan la mayor participación, con 255.266 registros.

Es importante aclarar que estos resultados representan registros de vehículos involucrados y no necesariamente accidentes únicos ni cantidades de víctimas.

### Comando de ejecución

```bash
spark-submit --master local[2] --driver-memory 1g analisis_siniestros.py
```

## 4. Procesamiento Streaming

Se implementó el programa `streaming_siniestros.py`, utilizando Apache Kafka y Spark Structured Streaming.

Se configuró el tópico `siniestros_colombia`, mediante el cual se transmitieron registros históricos en formato JSON para simular un flujo continuo de datos.

Spark recibe estos mensajes y genera estadísticas agrupadas según:

- Tipo de vehículo.
- Gravedad del accidente.
- Cantidad de mensajes procesados.

Los resultados se actualizan mediante microbatches.

### Comando de ejecución

```bash
spark-submit --master local[2] --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 streaming_siniestros.py
```

Para esta ejecución es necesario contar con Kafka en funcionamiento y el tópico correspondiente configurado.

## 5. Archivos del proyecto

| Archivo | Descripción |
|---|---|
| `analisis_siniestros.py` | Procesamiento batch y generación de resultados |
| `streaming_siniestros.py` | Procesamiento continuo mediante Kafka y Spark |
| `prueba_kafka.py` | Pruebas de conexión y funcionamiento de Kafka |

## 6. Evidencia audiovisual

Se realizó una grabación en la que se explica y demuestra el funcionamiento del procesamiento batch y Streaming, incluyendo la ejecución de comandos, la visualización de resultados estadísticos y la simulación de transmisión de datos.

**Video de demostración:**

[Ver video — Big Data, Tarea 3: Procesamiento Batch y Streaming](https://youtu.be/w45a9KWd5z4)

Enlace directo: https://youtu.be/w45a9KWd5z4

## 7. Conclusiones

El desarrollo de esta actividad permitió comprender la importancia de Apache Spark en el procesamiento y análisis de grandes conjuntos de datos.

Mediante el procesamiento batch fue posible analizar información histórica de siniestros viales en Colombia y generar resultados estadísticos y gráficos.

Por otra parte, la integración de Apache Kafka con Spark Structured Streaming permitió simular la llegada continua de mensajes y demostrar cómo estos pueden procesarse progresivamente.

Ambas metodologías constituyen herramientas importantes para el análisis de información y el apoyo a la toma de decisiones.

## 8. Fuente de datos

Datos Abiertos Colombia.

Portal oficial: https://www.datos.gov.co/

Conjunto de datos consultado mediante la API pública del portal.

---

**Elaborado por:** Edwin Andrés Gutiérrez Agudelo  
**Universidad Nacional Abierta y a Distancia — UNAD**  
**Año:** 2026
