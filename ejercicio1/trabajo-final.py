# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType
from pyspark.sql import functions as F
from pyspark.sql.functions import mean
import datetime
from pyspark.pandas import range
spark = (SparkSession.builder
        .appName("MySparkApp")
        .enableHiveSupport()
        .getOrCreate())

# %%


# %%
csvSchema = StructType([
    StructField("fecha", StringType(), True),
    StructField("horaUTC", StringType(), False),
    StructField("clase_de_vuelo", StringType(), True),
    StructField("clasificacion_de_vuelo", StringType(), True),
    StructField("tipo_de_movimiento", StringType(), True),
    StructField("aeropuerto", StringType(), True),
    StructField("origen_destino", StringType(), True),
    StructField("aerolinea_nombre", StringType(), True),
    StructField("aeronave", StringType(), True),
    StructField("pasajeros", IntegerType(), True)
])

# %%
"""
# Vuelos
"""

# %%
vuelos_2021 = (spark.read.option('header', 'true')
                         .option('delimiter', ';')
                         .csv('hdfs://172.17.0.2:9000/ingest/vuelos/2021-informe-ministerio.csv', schema=csvSchema))
vuelos_2022 = (spark.read.option('header', 'true')
                         .option('delimiter', ';')
                         .csv('hdfs://172.17.0.2:9000/ingest/vuelos/202206-informe-ministerio.csv', schema=csvSchema))
aeropuertos_detalle = (spark.read.option('header', 'true')
                            .option('delimiter', ';')
                            .csv('hdfs://172.17.0.2:9000/ingest/vuelos/aeropuertos_detalle.csv'))

# Unimos las dos tablas
vuelos = vuelos_2021.union(vuelos_2022)


# %%
vuelos.columns

# %%
vuelos.select('clase_de_vuelo', 'clasificacion_de_vuelo', 'origen_destino', 'aeronave', 'pasajeros').show(10)

# %%
vuelos_pasajeros_aeropuerto = (vuelos.filter(vuelos.clasificacion_de_vuelo == 'Domestico')
                               .groupby('aeropuerto')
                               .sum('pasajeros')
                               .orderBy('sum(pasajeros)', ascending=False))
vuelos_pasajeros_aeropuerto = vuelos_pasajeros_aeropuerto.withColumnRenamed('sum(pasajeros)', 'pasajeros')

vuelos_pasajeros_aeropuerto.show()

# %%
vuelos_pasajeros_aeropuerto.write.mode('overwrite').saveAsTable('aeropuertos.aeropuerto_tabla')

# %%
"""
# Aeropuertos detalle
"""

# %%
aeropuertos_detalle.columns

# %%
aeropuertos_detalle.select('condicion').show()

# %%
# Cantidad de aeropuertos públicos por provincia
aeropuertos_publicos_provincia = (aeropuertos_detalle.filter(aeropuertos_detalle.condicion == 'PUBLICO')
                                  .groupBy('provincia')
                                  .count()
                                  .orderBy('count', ascending=False))
                                  

# %%
aeropuertos_publicos_provincia.write.mode('overwrite').saveAsTable('aeropuertos.aeropuerto_detalles_tabla')

# %%
"""

"""

# %%
