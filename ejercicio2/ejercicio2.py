# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType
from pyspark.sql import functions as F
from pyspark.sql.functions import mean
from pyspark.sql.functions import to_timestamp
import datetime

spark = (SparkSession.builder
        .appName("MySparkApp")
        .enableHiveSupport()
        .getOrCreate())

# %%
CarRentalData = (spark.read.option('header', 'true')
                      .csv('hdfs://172.17.0.2:9000/ingest/automobiles/CarRentalData.csv'))
georef_usa_state = (spark.read.option('header', 'true')
                         .option('delimiter', ';')
                         .csv('hdfs://172.17.0.2:9000/ingest/automobiles/georef-united-states-of-america.csv'))

# %%
# Problema 3

# %%
CarRentalData.columns

# %%
CarRentalData = (CarRentalData.withColumnRenamed('location.city', 'city')
                              .withColumnRenamed('location.country', 'country')
                              .withColumnRenamed('location.latitude', 'latitude')
                              .withColumnRenamed('location.longitude', 'longitude')
                              .withColumnRenamed('location.state', 'state')
                              .withColumnRenamed('owner.id', 'owner_id')
                              .withColumnRenamed('rate.daily', 'rate_daily')
                              .withColumnRenamed('vehicle.make', 'vehicle_make')
                              .withColumnRenamed('vehicle.model', 'vehicle_model')
                              .withColumnRenamed('vehicle.type', 'vehicle_type')
                              .withColumnRenamed('vehicle.year', 'vehicle_year'))
                

# %%
CarRentalData.columns


# %%
"""
Vemos que las columnas **rating**
"""

# %%
CarRentalData = CarRentalData.withColumn("rating", F.round(F.col("rating")).cast(IntegerType()))
CarRentalData = (CarRentalData.withColumn('renterTripsTaken', F.col('renterTripsTaken').cast(IntegerType()))
                              .withColumn('reviewCount', F.col('reviewCount').cast(IntegerType()))
                              .withColumn('latitude', F.col('latitude').cast(FloatType()))
                              .withColumn('longitude', F.col('longitude').cast(FloatType()))
                              .withColumn('owner_id', F.col('owner_id').cast(IntegerType()))
                              .withColumn('rate_daily', F.col('rate_daily').cast(IntegerType()))
                              .withColumn('vehicle_year', F.col('vehicle_year').cast(IntegerType())))

# %%
georef_usa_state = (georef_usa_state.withColumnRenamed('Geo Point', 'geo_point')
                                    .withColumnRenamed('Geo Shape', 'geo_shape')
                                    .withColumnRenamed('Official Code State', 'OCS')
                                    .withColumnRenamed('Official Name State', 'ONS')
                                    .withColumnRenamed('Iso 3166-3 Area Code', 'iso_3166_3_ac')
                                    .withColumnRenamed('Type', 'type')
                                    .withColumnRenamed('United States Postal Service state abbreviation', 'postal_cod')
                                    .withColumnRenamed('State FIPS Code', 'FIPS')
                                    .withColumnRenamed('State GNIS Code', 'GNIS'))
georef_usa_state = (georef_usa_state.withColumn('Year', F.col('Year').cast(IntegerType()))
                                    .withColumn('OCS', F.col('OCS').cast(IntegerType())))

# %%
df = CarRentalData.join(georef_usa_state, on='state') n
df = df.na.drop(subset=['rating'])
df = df.withColumn('fuelType', F.lower('fuelType'))
df = df.filter(df.state != 'TX')

# %%
df.write.mode('overwrite').saveAsTable('car_rental_db.car_rental_analytics')

# %%
