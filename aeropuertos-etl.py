# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType
from pyspark.sql.functions import col, to_date, when

# %%
spark = (SparkSession.builder
        .appName("aeropuertos")
        .enableHiveSupport()
        .getOrCreate())

# %%
"""
# Aeropuertos
"""

# %%
aeropuertos_2021 = (spark.read.option('header', 'true')
                         .option('delimiter', ';')
                         .csv('hdfs://172.17.0.2:9000/ingest/vuelos/2021-informe-ministerio.csv'))
aeropuertos_2022 = (spark.read.option('header', 'true')
                         .option('delimiter', ';')
                         .csv('hdfs://172.17.0.2:9000/ingest/vuelos/202206-informe-ministerio.csv'))

# Unimos las dos tablas
aeropuertos = aeropuertos_2021.unionByName(aeropuertos_2022)

# %%
aeropuertos.columns

# %%
# Cambiamos los nombres de las columnas
aeropuertos = aeropuertos.withColumnRenamed('Fecha', 'fecha')
aeropuertos = aeropuertos.withColumnRenamed('Hora UTC', 'horaUTC')
aeropuertos = aeropuertos.withColumnRenamed('Clase de Vuelo (todos los vuelos)', 'clase_de_vuelo')
aeropuertos = aeropuertos.withColumnRenamed('Clasificación Vuelo', 'clasificacion_vuelo')
aeropuertos = aeropuertos.withColumnRenamed('Tipo de Movimiento', 'tipo_de_movimiento')
aeropuertos = aeropuertos.withColumnRenamed('Aeropuerto', 'aeropuerto')
aeropuertos = aeropuertos.withColumnRenamed('Origen / Destino', 'origen_destino')
aeropuertos = aeropuertos.withColumnRenamed('Aerolinea Nombre', 'aerolinea_nombre')
aeropuertos = aeropuertos.withColumnRenamed('Aeronave', 'aeronave')
aeropuertos = aeropuertos.withColumnRenamed('Pasajeros', 'pasajeros')
aeropuertos = aeropuertos.withColumnRenamed('Calidad dato', 'calidad_dato')

# %%
# Casteamos fecha a date y pasajeros a int
aeropuertos = aeropuertos.withColumn("fecha", to_date(col("fecha"), "dd/MM/yyyy"))
aeropuertos = aeropuertos.withColumn("pasajeros", col("pasajeros").cast(IntegerType()))

# Convertimos Domestico -> Doméstico en todo el df
aeropuertos = aeropuertos.withColumn("clasificacion_vuelo", 
                                     when(col("clasificacion_vuelo") == 'Domestico',
                                                  'Doméstico').otherwise(col("clasificacion_vuelo")))

# %%
# Hacemos la transformación
aeropuertos = aeropuertos.drop('calidad_dato')
aeropuertos = aeropuertos.filter(aeropuertos.clasificacion_vuelo == 'Doméstico')
aeropuertos = aeropuertos.fillna(0, subset='pasajeros')

# %%
aeropuertos.printSchema()

# %%
aeropuertos.write.mode('overwrite').saveAsTable('aeropuertos.aeropuerto_tabla')

# %%
"""
# Aeropuertos detalle
"""

# %%
aeropuerto_detalles = (spark.read.option('header', 'true')
                            .option('delimiter', ';')
                            .csv('hdfs://172.17.0.2:9000/ingest/vuelos/aeropuertos_detalle.csv'))



# %%
# Casteamos las columnas elev y distancia_ref a float
aeropuerto_detalles = aeropuerto_detalles.withColumn("elev", col("elev").cast(FloatType()))
aeropuerto_detalles = aeropuerto_detalles.withColumn("distancia_ref", col("distancia_ref").cast(FloatType()))
                                                                                

# %%
aeropuerto_detalles.select('ref').show(5)

# %%
aeropuerto_detalles.columns

# %%
# Renombramos las columnas local -> aeropuerto y oaci -> oac
aeropuerto_detalles = aeropuerto_detalles.withColumnRenamed('local', 'aeropuerto')
aeropuerto_detalles = aeropuerto_detalles.withColumnRenamed('oaci', 'oac')


# %%
# Aplicamos la transformación
aeropuerto_detalles = aeropuerto_detalles.drop('inhab', 'fir')
aeropuerto_detalles = aeropuerto_detalles.fillna(0, subset='distancia_ref')


# %%
aeropuerto_detalles.printSchema()

# %%
"""
La ciudad del aeropuerto está en la columna `ref`:
"""

# %%
# Guardamos la tabla en Hive
aeropuerto_detalles.write.mode('overwrite').saveAsTable('aeropuertos.aeropuerto_detalles_tabla')

# %%
