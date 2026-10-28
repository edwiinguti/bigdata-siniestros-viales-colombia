
from pyspark.sql import SparkSession, functions as F
from pathlib import Path
import csv
import html

# TAREA 3 - BIG DATA - GRUPO 76
# Analisis de vehiculos involucrados en siniestros viales
# Colombia, Valle del Cauca y Palmira
# Fuente: Ministerio de Transporte / RUNT

BASE = Path.home() / "BIGDATA_TAREA3"
DATOS = BASE / "datos" / "siniestros_colombia.csv"
RESULTADOS = BASE / "resultados"
GRAFICOS = BASE / "graficos"

RESULTADOS.mkdir(parents=True, exist_ok=True)
GRAFICOS.mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("Tarea3_Siniestros_Viales_Colombia")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def guardar_csv(nombre, columnas, filas):
    ruta = RESULTADOS / nombre
    with open(ruta, "w", encoding="utf-8-sig", newline="") as f:
        escritor = csv.writer(f)
        escritor.writerow(columnas)
        escritor.writerows(filas)
    print("Guardado:", ruta)


def guardar_grafico(nombre, titulo, filas):
    # Grafico SVG sin dependencias externas
    filas = [(str(k), int(v)) for k, v in filas[:12]]
    ancho = 1050
    alto = 120 + 44 * len(filas)
    mayor = max([v for _, v in filas], default=1) or 1
    elementos = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{ancho}" height="{alto}" '
        f'viewBox="0 0 {ancho} {alto}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="35" y="40" font-family="Arial" '
        f'font-size="22" font-weight="bold">'
        f'{html.escape(titulo)}</text>'
    ]

    for i, (etiqueta, valor) in enumerate(filas):
        y = 65 + i * 44
        longitud = round(530 * valor / mayor)
        elementos.append(
            f'<text x="35" y="{y+23}" font-family="Arial" '
            f'font-size="14">{html.escape(etiqueta[:35])}</text>'
        )
        elementos.append(
            f'<rect x="340" y="{y+4}" width="{longitud}" '
            f'height="26" rx="4" fill="#087e8b"/>'
        )
        elementos.append(
            f'<text x="{355+longitud}" y="{y+23}" '
            f'font-family="Arial" font-size="14">'
            f'{valor:,}</text>'
        )

    elementos.append("</svg>")
    ruta = GRAFICOS / nombre
    ruta.write_text("\n".join(elementos), encoding="utf-8")
    print("Grafico:", ruta)


def agrupar(tabla, campo, nombre, limite=20):
    filas = [
        (r[campo], r["total"])
        for r in tabla.groupBy(campo)
        .agg(F.count("*").alias("total"))
        .orderBy(F.desc("total"))
        .limit(limite)
        .collect()
    ]
    guardar_csv(nombre, [campo, "total"], filas)
    return filas


print("\n=== CARGA DEL CONJUNTO DE DATOS ===")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("mode", "FAILFAST")
    .csv(str(DATOS))
)

total_original = df.count()
print("Registros originales:", total_original)
print("Columnas:", len(df.columns))
df.printSchema()

# Revision de valores ausentes antes de limpiar
print("\n=== CALIDAD DE DATOS ===")

faltantes = df.select([
    F.sum(
        F.when(
            F.col(c).isNull() |
            (F.trim(F.col(c).cast("string")) == ""),
            1
        ).otherwise(0)
    ).alias(c)
    for c in df.columns
]).first().asDict()

guardar_csv(
    "calidad_valores_faltantes.csv",
    ["columna", "valores_faltantes"],
    list(faltantes.items())
)

# Coincidencias exactas: no se eliminan automaticamente
filas_iguales = total_original - df.dropDuplicates().count()
print("Filas exactamente repetidas:", filas_iguales)
print("No se eliminan sin verificar su significado.")

# Normalizacion de variables categoricas
categoricas = [
    "marca_vehiculo",
    "tipo_vehiculo",
    "gravedad_accidente",
    "departamento_accidente",
    "municipio_accidente",
    "autoridad_de_transito"
]

for c in categoricas:
    df = df.withColumn(
        c,
        F.when(
            F.col(c).isNull() |
            (F.trim(F.col(c)) == ""),
            F.lit("NO REGISTRADO")
        ).otherwise(
            F.upper(F.regexp_replace(F.trim(F.col(c)), r"\s+", " "))
        )
    )

# Fechas: la fuente puede usar MM/yyyy, dd/MM/yyyy o yyyy-MM-dd
fecha = F.trim(F.col("fecha_accidente"))

df = df.withColumn(
    "fecha_procesada",
    F.when(
        fecha.rlike(r"^\d{2}/\d{4}$"),
        F.to_date(F.concat(F.lit("01/"), fecha), "dd/MM/yyyy")
    ).when(
        fecha.rlike(r"^\d{2}/\d{2}/\d{4}$"),
        F.to_date(fecha, "dd/MM/yyyy")
    ).when(
        fecha.rlike(r"^\d{4}-\d{2}-\d{2}$"),
        F.to_date(fecha, "yyyy-MM-dd")
    )
)

df = (
    df.withColumn("anio", F.year("fecha_procesada"))
    .withColumn("mes", F.month("fecha_procesada"))
)

fechas_invalidas = df.filter(
    F.col("fecha_procesada").isNull()
).count()

edades_invalidas = df.filter(
    F.col("edad_vehiculo").isNotNull() &
    (
        (F.col("edad_vehiculo") < 0) |
        (F.col("edad_vehiculo") > 150)
    )
).count()

print("Fechas no interpretadas:", fechas_invalidas)
print("Edades fuera del rango de revision:", edades_invalidas)

# Las fechas y edades dudosas se conservan para auditoria
df.cache()
total = df.count()

print("\n=== DISTRIBUCION GEOGRAFICA ===")

valle = df.filter(
    F.col("departamento_accidente") == "VALLE DEL CAUCA"
)

palmira = valle.filter(
    F.col("municipio_accidente") == "PALMIRA"
)

n_valle = valle.count()
n_palmira = palmira.count()

geografia = [
    ("Colombia", total, 100.0),
    ("Valle del Cauca", n_valle, round(n_valle / total * 100, 2)),
    ("Palmira", n_palmira, round(n_palmira / total * 100, 2))
]

for fila in geografia:
    print(fila)

guardar_csv(
    "comparacion_geografica.csv",
    ["ubicacion", "vehiculos", "porcentaje_nacional"],
    geografia
)

guardar_grafico(
    "comparacion_geografica.svg",
    "Vehiculos involucrados por zona",
    [(r[0], r[1]) for r in geografia]
)

print("\n=== TIPOS DE VEHICULOS ===")

tipos_nacional = agrupar(
    df, "tipo_vehiculo", "tipos_vehiculo_colombia.csv"
)
tipos_valle = agrupar(
    valle, "tipo_vehiculo", "tipos_vehiculo_valle.csv"
)
tipos_palmira = agrupar(
    palmira, "tipo_vehiculo", "tipos_vehiculo_palmira.csv"
)

guardar_grafico(
    "tipos_vehiculo_colombia.svg",
    "Tipos de vehiculos - Colombia",
    tipos_nacional[:10]
)

guardar_grafico(
    "tipos_vehiculo_palmira.svg",
    "Tipos de vehiculos - Palmira",
    tipos_palmira[:10]
)

print("\n=== GRAVEDAD DE LOS SINIESTROS ===")

gravedad_nacional = agrupar(
    df, "gravedad_accidente", "gravedad_colombia.csv"
)
gravedad_valle = agrupar(
    valle, "gravedad_accidente", "gravedad_valle.csv"
)
gravedad_palmira = agrupar(
    palmira, "gravedad_accidente", "gravedad_palmira.csv"
)

guardar_grafico(
    "gravedad_colombia.svg",
    "Clasificacion de gravedad - Colombia",
    gravedad_nacional
)

print("\n=== ANALISIS TEMPORAL ===")

validas = df.filter(F.col("fecha_procesada").isNotNull())

agrupacion_anual = agrupar(
    validas, "anio", "registros_por_anio.csv", 50
)

agrupacion_mensual = [
    (r["anio"], r["mes"], r["total"])
    for r in validas.groupBy("anio", "mes")
    .agg(F.count("*").alias("total"))
    .orderBy("anio", "mes")
    .collect()
]

guardar_csv(
    "registros_por_anio_mes.csv",
    ["anio", "mes", "total"],
    agrupacion_mensual
)

guardar_grafico(
    "registros_por_anio.svg",
    "Registros de vehiculos por anio",
    sorted(agrupacion_anual)
)

print("\n=== DEPARTAMENTOS Y MUNICIPIOS ===")

departamentos = agrupar(
    df, "departamento_accidente",
    "departamentos_colombia.csv", 50
)

municipios_valle = agrupar(
    valle, "municipio_accidente",
    "municipios_valle.csv", 100
)

guardar_grafico(
    "departamentos_colombia.svg",
    "Departamentos con mas registros",
    departamentos[:10]
)

guardar_grafico(
    "municipios_valle.svg",
    "Municipios del Valle del Cauca",
    municipios_valle[:10]
)

print("\n=== RESUMEN FINAL ===")

guardar_csv(
    "resumen_general.csv",
    ["indicador", "valor"],
    [
        ("registros_originales", total_original),
        ("registros_procesados", total),
        ("registros_valle", n_valle),
        ("registros_palmira", n_palmira),
        ("filas_exactamente_repetidas", filas_iguales),
        ("fechas_no_interpretadas", fechas_invalidas),
        ("edades_fuera_rango", edades_invalidas)
    ]
)

print("Total Colombia:", total)
print("Total Valle del Cauca:", n_valle)
print("Total Palmira:", n_palmira)
print("Resultados CSV:", RESULTADOS)
print("Graficos SVG:", GRAFICOS)
print("\nPROCESAMIENTO BATCH FINALIZADO")

df.unpersist()
spark.stop()
