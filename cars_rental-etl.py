# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType
from pyspark.sql.functions import col, round, lower

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
                              .withColumnRenamed('vehicle.make', 'make')
                              .withColumnRenamed('vehicle.model', 'model')
                              .withColumnRenamed('vehicle.type', 'type')
                              .withColumnRenamed('vehicle.year','year'))
                

# %%
"""
Vemos que las columnas **rating**
"""

# %%
CarRentalData = CarRentalData.withColumn("rating", round(col("rating")).cast(IntegerType()))
CarRentalData = (CarRentalData.withColumn('renterTripsTaken', col('renterTripsTaken').cast(IntegerType()))
                              .withColumn('reviewCount', col('reviewCount').cast(IntegerType()))
                              .withColumn('latitude', col('latitude').cast(FloatType()))
                              .withColumn('longitude', col('longitude').cast(FloatType()))
                              .withColumn('owner_id', col('owner_id').cast(IntegerType()))
                              .withColumn('rate_daily', col('rate_daily').cast(IntegerType()))
                              .withColumn('year', col('year').cast(IntegerType())))
                              

# %%
georef_usa_state = (georef_usa_state.withColumnRenamed('Geo Point', 'geo_point')
                                    .withColumnRenamed('Geo Shape', 'geo_shape')
                                    .withColumnRenamed('Official Code State', 'OCS')
                                    .withColumnRenamed('Official Name State', 'ONS')
                                    .withColumnRenamed('Iso 3166-3 Area Code', 'iso_3166_3_ac')
                                    .withColumnRenamed('Type', 'type')
                                    .withColumnRenamed('United States Postal Service state abbreviation', 'state')
                                    .withColumnRenamed('State FIPS Code', 'FIPS')
                                    .withColumnRenamed('State GNIS Code', 'GNIS')
                                    .withColumnRenamed('Year', 'georef_year'))
georef_usa_state = (georef_usa_state.withColumn('geo_point', col('geo_point').cast(IntegerType()))
                                    .withColumn('georef_year', col('georef_year').cast(IntegerType()))
                                    .withColumn('OCS', col('OCS').cast(IntegerType()))
                                    .withColumn('GNIS', col('GNIS').cast(IntegerType())))

# %%
georef_usa_state.select('*').show(5)

# %%
georef_usa_state = georef_usa_state.drop('type')
df = CarRentalData.join(georef_usa_state, on='state') 
df = df.na.drop(subset=['rating'])
df = df.withColumn('fuelType', lower('fuelType'))
df = df.filter(df.state != 'TX')

# %%
df.write.mode('overwrite').saveAsTable('car_rental_db.car_rental_analytics')

# %%
