# Examen Final
1. Hacer ingest de los siguientes files relacionados con transporte aéreo de Argentina : 

2021: https://data-engineer-edvai-public.s3.amazonaws.com/2021-informe-ministerio.csv


2022: https://data-engineer-edvai-public.s3.amazonaws.com/202206-informe-ministerio.csv


Aeropuertos_detalles:
https://data-engineer-edvai-public.s3.amazonaws.com/aeropuertos_detalle.csv

Esto se realiza en el archivo [aeropuertos-ingest.sh](~/examen-final/aeropuertos-ingest.sh)

2. Las tablas fueron creadas en Hive.

3. Realizar un proceso automático orquestado por airflow que ingeste los archivos
previamente mencionados entre las fechas 01/01/2021 y 30/06/2022 en las dos
columnas creadas.


El dag para orquestar el proceso en Airflow se encuentra en [aeropuertos.py](./dags/aeropuertos.py).

4. Realizar las siguiente transformaciones en los pipelines de datos:

- eliminar la columna inhab ya que no se utilizará para el análisis
-​ eliminar la columna fir ya que no se utilizará para el análisis
-​ eliminar la columna “calidad del dato” ya que no se utilizará para el análisis
- Filtrar los vuelos internacionales ya que solamente se analizarán los vuelos domésticos
- En el campo pasajeros si se encuentran campos en Null convertirlos en 0 (cero)
- En el campo distancia_ref si se encuentran campos en Null convertirlos en 0 (cero)

Estas transformaciones se realizan en el archivo [aeropuertos-etl.py](./aeropuertos-etl.py):
```
aeropuertos = aeropuertos.withColumn("clasificacion_vuelo", 
                                     when(col("clasificacion_vuelo") == 'Domestico',
                                                  'Doméstico').otherwise(col("clasificacion_vuelo")))
aeropuerto_detalles = aeropuerto_detalles.drop('inhab', 'fir')
aeropuerto_detalles = aeropuerto_detalles.fillna(0, subset='distancia_ref')
aeropuertos = aeropuertos.drop('calidad_dato')
aeropuertos = aeropuertos.filter(aeropuertos.clasificacion_vuelo == 'Doméstico')
aeropuertos = aeropuertos.fillna(0, subset='pasajeros')
```

5. Schema de las tablas


![Schema de aeropuerto-tabla](./images/aeropuerto-schema.png)


![Schema de aeropuerto-detalles](./images/aeropuerto-detalles-schema.png)

6.​ Determinar la cantidad de vuelos entre las fechas 01/12/2021 y 31/01/2022. Mostrar consulta y Resultado de la query	

```
SELECT COUNT(*) 
FROM aeropuerto_tabla
WHERE fecha >= '2021-12-01' AND fecha <= '2022-01-31';

OK
57984
```

7.​ Cantidad de pasajeros que viajaron en Aerolíneas Argentinas entre el 01/01/2021 y 30/06/2022. Mostrar consulta y Resultado de la query
```
SELECT SUM(pasajeros)
FROM aeropuerto_tabla
WHERE fecha >= '2021-01-01' AND 
      fecha <= '2022-06-30' AND 
      aerolinea_nombre = 'AEROLINEAS ARGENTINAS SA';

7484860
```


8\. Mostrar fecha, hora, código aeropuerto salida, ciudad de salida, código de aeropuerto de arribo, ciudad de arribo, 
y cantidad de pasajeros de cada vuelo, entre el 01/01/2022 y el 30/06/2022 ordenados por fecha de manera 
descendiente. Mostrar consulta y Resultado de la query.

```
WITH salida AS (
   SELECT aeropuerto, ref
   FROM aeropuerto_detalles_tabla
), arribo AS (
   SELECT aeropuerto, ref
   FROM aeropuerto_detalles_tabla
)
SELECT fecha,
	   horaUTC,
       salida.aeropuerto AS aeropuerto_salida,
       salida.ref AS ciudad_salida,
       arribo.aeropuerto AS aeropuerto_arribo,
       arribo.ref AS ciudad_arribo,
       pasajeros
FROM aeropuerto_tabla
JOIN salida ON salida.aeropuerto = aeropuerto_tabla.aeropuerto AND
     aeropuerto_tabla.tipo_de_movimiento = 'Despegue'
JOIN arribo ON arribo.aeropuerto = aeropuerto_tabla.origen_destino    
WHERE fecha >= '2022-01-01' AND fecha <= '2022-06-30' 
ORDER BY fecha DESC, horaUTC DESC
```

|fecha|horautc|aeropuerto_salida|ciudad_salida|aeropuerto_arribo|ciudad_arribo|pasajeros|
|-----|-------|-----------------|-------------|-----------------|-------------|---------|
|2022-06-30|23:59|DOZ|Mendoza|AER|Ciudad de Buenos Aires|0|
|2022-06-30|23:49|DOZ|Mendoza|CBA|Córdoba|0|
|2022-06-30|23:48|AER|Ciudad de Buenos Aires|CBA|Córdoba|0|
|2022-06-30|23:47|CBA|Córdoba|AER|Ciudad de Buenos Aires|90|
|2022-06-30|23:39|AER|Ciudad de Buenos Aires|SAL|Salta|87|
|2022-06-30|23:37|ROS|Rosario|AER|Ciudad de Buenos Aires|42|
|2022-06-30|23:37|AER|Ciudad de Buenos Aires|BAR|San Carlos de Bariloche|0|
|2022-06-30|23:35|GAL|Río Gallegos|AER|Ciudad de Buenos Aires|0|
|2022-06-30|23:30|AER|Ciudad de Buenos Aires|SDE|Santiago del Estero|86|
|2022-06-30|23:28|BAR|San Carlos de Bariloche|AER|Ciudad de Buenos Aires|0|
|2022-06-30|23:25|OSA|Santa Rosa|EZE|Capital Federal|1|
|2022-06-30|23:22|SAL|Salta|EZE|Capital Federal|41|
|2022-06-30|23:22|AER|Ciudad de Buenos Aires|BAR|San Carlos de Bariloche|0|
|2022-06-30|23:21|IGU|Cataratas del Iguazú|EZE|Capital Federal|37|
|2022-06-30|23:20|SRA|San Rafael|SRA|San Rafael|0|


9\. Cuales son las 10 aerolíneas que más pasajeros llevaron entre el 01/01/2021 y el
30/06/2022 exceptuando aquellas aerolíneas que no tengan nombre. Mostrar consulta y
Visualización.
```
SELECT aerolinea_nombre AS aerolinea, SUM(pasajeros) AS pasajeros
FROM aeropuerto_tabla
WHERE fecha >= '2021-01-01' AND fecha <= '2022-06-30'
GROUP BY aerolinea_nombre
ORDER BY pasajeros DESC
LIMIT 10;
```


|aerolinea|total_pasajeros|
|---------|---------------|
|AEROLINEAS ARGENTINAS SA|7.484.860|
|JETSMART AIRLINES S.A.|1.511.650|
|FB LÍNEAS AÉREAS - FLYBONDI|1.482.473|
|AMERICAN JET S.A.|25.789|
|L.A.D.E.|15.074|
|BAIRES FLY SA|4.960|
|LADE|3.895|
|FUERZA AEREA ARGENTINA|3.855|
|FUERZA AEREA ARGENTINA (FAA)|3.138|
|FLYING AMERICA SA|2.839|


![Las 10 aerolíneas que más pasajeros llevaron entre el 01/01/2021 y el
30/06/2022 exceptuando aquellas aerolíneas que no tengan nombre.](./images/query9.png)


10\.​Cuales son las 10 aeronaves más utilizadas entre el 01/01/2021 y el 30/06/22 que
despegaron desde la Ciudad autónoma de Buenos Aires o de Buenos Aires,
exceptuando aquellas aeronaves que no cuentan con nombre.

```
SELECT aeronave, COUNT(aeronave) AS count
FROM aeropuerto_tabla
JOIN aeropuerto_detalles_tabla ON 
    aeropuerto_detalles_tabla.aeropuerto = aeropuerto_tabla.aeropuerto
WHERE fecha >= '2021-01-01' AND 
      fecha <= '2022-06-30' AND
      aeronave != '0' AND
      tipo_de_movimiento != 'Despegue'
      AND (provincia  = 'BUENOS AIRES' OR 
           provincia = 'CIUDAD AUTONOMA DE BUENOS AIRES')
GROUP BY aeronave 
ORDER BY count DESC
LIMIT 10;
```


|aeronave|despegues|
|--------|-----|
|CE-150-L|7.930|
|CE-152|7.866|
|CE-150-M|5.922|
|EMB-ERJ190100IGW|2.965|
|CE-150-J|2.962|
|CE-150-G|2.784|
|PA-PA-28-181|2.434|
|CE-152-II|1.957|
|TEA-P2002 SIERRA|1.835|
|CE-150-K|1.818|


![](./images/query10.png)





11\. Agregaría a este dataset si el aeropuerto está en 
una ciudad turística. En tal caso se podría predecir cuáles son las temporadas altas y bajas.

12\. Elabore sus conclusiones y recomendaciones sobre este proyecto.

El Data Warehouse es muy rico y se pueden estudiar muchas cuestiones a partir del mismo. Por ejemplo, podemos estudiar las temporadas altas y bajas de las ciudades turísticas. Por ejemplo, consideremos el número de pasajeros de los nueve aeropuertos con mayor tráfico de pasajeros excepto Aeroparque y Ezeiza,
agrupados por año, mes y aeropuerto 

```
WITH aeropuerto_ciudad AS (
	SELECT aeropuerto, REF
	FROM aeropuerto_detalles_tabla
), aeropuertos_alto_trafico AS (
	SELECT aeropuerto, SUM(pasajeros) AS pasajeros
	FROM aeropuerto_tabla
	WHERE aeropuerto != 'AER' AND aeropuerto != 'EZE'
	GROUP BY aeropuerto 
	ORDER BY pasajeros DESC
	LIMIT 9
)
SELECT aeropuerto_tabla.aeropuerto,
	   REF AS ciudad,
	   YEAR(fecha) anho,
       MONTH(fecha) mes,
       SUM(pasajeros) AS pasajeros
FROM aeropuerto_tabla
JOIN aeropuerto_ciudad ON aeropuerto_ciudad.aeropuerto = aeropuerto_tabla.aeropuerto
WHERE aeropuerto_ciudad.aeropuerto IN 
	(SELECT aeropuerto FROM aeropuertos_alto_trafico)
GROUP BY aeropuerto_tabla.aeropuerto, ref, YEAR(fecha), MONTH(fecha)
ORDER BY aeropuerto, anho, mes
```

![Temporadas altas y bajas](./images/temporadas.png)

Podemos observar que el número de pasajeros es menor entre los meses de abril y junio, y mayor entre diciembre y febrero.


# Ejercicio 2
1.​ La tabla fue creada en Hive.
2. Crear script para el ingest de estos dos files
https://data-engineer-edvai-public.s3.amazonaws.com/CarRentalData.csv
https://data-engineer-edvai-public.s3.amazonaws.com/georef-united-states-of-america-state.csv

El script que realiza la ingesta se encuentra en [cars_rental-ingest.sh](./cars_rental-ingest.sh)

3. Crear un script para tomar el archivo desde HDFS y hacer las siguientes
transformaciones:
- En donde sea necesario, modificar los nombres de las columnas. Evitar espacios
y puntos (reemplazar por _ ). Evitar nombres de columna largos
- Redondear los float de ‘rating’ y castear a int.
- Joinear ambos files
- Eliminar los registros con rating nulo
- Cambiar mayúsculas por minúsculas en ‘fuelType’
- Excluir el estado Texas

El script que realiza estas tareas es [cars_rental-etl.py](./cars_rental-etl.py)

4. Realizar un proceso automático en Airflow que orqueste los pipelines creados en los puntos anteriores.

El dag que realiza la orquestación se encuentra en [cars-rental.py](./dags/cars-rental.py)

5. Por medio de consultas SQL al data-warehouse, mostrar:
a. Cantidad de alquileres de autos, teniendo en cuenta sólo los vehículos
ecológicos (fuelType hibrido o eléctrico) y con un rating de al menos 4.
```
SELECT SUM(renterTripsTaken)
FROM car_rental_analytics
WHERE (fueltype = 'hybrid' OR fueltype = 'electric')
     AND rating >= 4
```
26949

b. los 5 estados con menor cantidad de alquileres (mostrar query y visualización).

```
SELECT state, SUM(renterTripsTaken) AS rent_number
FROM car_rental_analytics
GROUP BY state
ORDER BY rent_number ASC
LIMIT 5
```
|state|rent_number|
|-----|-----------|
|MT|7|
|AR|43|
|NH|51|
|WV|76|
|MS|107|



![](images/ej2-query-b.png)

c. los 10 modelos (junto con su marca) de autos más rentados (mostrar query y
visualización).

```
SELECT model, 
       make, 
       SUM(renterTripsTaken) AS rent_count
FROM car_rental_analytics
GROUP BY model, make 
ORDER BY rent_count DESC
LIMIT 10
```
|model|make|rent_count|
|-----|----|----------|
|Model 3|Tesla|9.794|
|Mustang|Ford|5.882|
|Wrangler|Jeep|4.762|
|Corolla|Toyota|4.676|
|Corvette|Chevrolet|4.164|
|Model S|Tesla|3.952|
|Model X|Tesla|3.638|
|3 Series|BMW|3.293|
|C-Class|Mercedes-Benz|2.818|
|Camaro|Chevrolet|2.797|


![](images/ej2-query-c.png)

d. El dataset no contiene el campo `year`, por lo cual el análisis no se puede realizar.

![](./images/car_rental.schema.png)
(la columna `year` es el año de fabricación del vehículo)

e. las 5 ciudades con más alquileres de vehículos ecológicos (fuelType hibrido o eléctrico)
```
SELECT city, SUM(renterTripsTaken) AS ecologic_vehicles
FROM car_rental_analytics 
WHERE fueltype = 'hybrid' OR fueltype = 'electric'
GROUP BY city
ORDER BY ecologic_vehicles DESC
LIMIT 5
```
|city|ecologic_vehicles|
|----|-----------------|
|San Diego|1793|
|Las Vegas|1551|
|Los Angeles|1075|
|San Francisco|1058|
|Portland|928|


![](images/ej2-query-e.png)

f. el promedio de reviews, segmentando por tipo de combustible
```
SELECT fueltype, AVG(reviewcount)
FROM car_rental_analytics
GROUP BY fueltype
```

|fuelType| AVG(reviewCount)|
|------|----------------|
|NULL|	21.049180327868854|
|diesel|	17.5 |
|electric	|28.339483394833948 |
|gasoline |	31.927023661270237 |
|hybrid	 |34.87336244541485 |

6. Elabore sus conclusiones y recomendaciones sobre este proyecto.

En este proyecto se presenta un Data Warehouse de autos alquilados con distintas variables.
Es interesante porque tiene un campo que representa el tipo de combustible
del auto alquilado. Si contamos la cantidad de autos alquilados por tipo de combustible
```
SELECT fuelType, SUM(renterTripsTaken) total_rent
FROM car_rental_analytics
WHERE fuelType != ''
GROUP BY fuelType
ORDER BY ecologic_count DESC
```
|fueltype|total_rent|
|--------|--------------|
|gasoline|151.022|
|electric|17.601|
|hybrid|9.348|
|diesel|1.194|

![Total de vehiculos alquilados por tipo de combustible](./images/ej2-fueltype-rent.png)

Vemos que la mayoría de los vehículos alquilados usan gasolina como combustible. Lamentablemente no disponemos de la columna `year`, que
nos permitiría estudiar la evolución del tipo de combustible de los
autos alquilados en función del tiempo.

Otra cuestión que podríamos investigar es la cantidad de autos alquilados
según el año de fabricación del vehículo. Por ejemplo, busquemos la cantidad
de alquileres según el año de fabricación para vehículos fabricados a partir
del año 2010:
```
SELECT year, SUM(renterTripsTaken) AS rent_per_year
FROM car_rental_analytics
WHERE year >= 2010
GROUP BY year
ORDER BY year
```
|year|rent_per_year|
|----|-------------|
|2005|1485|
|2006|2705|
|2007|3885|
|2008|5654|
|2009|3572|
|2010|6754|
|2011|8141|
|2012|9999|
|2013|12328|
|2014|15477|
|2015|18799|
|2016|21684|
|2017|21908|
|2018|26868|
|2019|17632|
|2020|3339|

![Alquileres según el año de fabricación del vehículo](./images/ej2-rent-per-year.png)

Vemos que la máxima cantidad de alquileres se da para vehículos fabricados entre el 2015 y el 2019.

# Ejercicio 3 - Dataprep
1\.​ ¿Para que se utiliza data prep?

Dataprep, es una herramienta diseñada para simplificar y acelerar el proceso de preparación de datos.  Su propósito fundamental es permitir a analistas de datos, científicos de datos y usuarios de negocio, explorar, limpiar, transformar y enriquecer grandes volúmenes de datos crudos y desestructurados, dejándolos listos para el análisis, la generación de informes y la aplicación en modelos de aprendizaje automático (machine learning).

2\.​ ¿Qué cosas se pueden realizar con DataPrep?

Exploración y perfilado visual de datos, limpieza de datos intuitiva, transformación de datos
sin código, enriquecimiento de datos.

3\.​ ¿Por qué otra/s herramientas lo podrías reemplazar? Por qué?

Dataproc es una alternativa si se trabaja con grandes volúmenes de datos,ya que está basado en Hadoop y Spark.

4\.​ ¿Cuáles son los casos de uso comunes de Data Prep de GCP?

El caso de uso más fundamental de Dataprep es la creación de flujos de trabajo de Extracción, Transformación y Carga (ETL) o Extracción, Carga y Transformación (ELT).

5\.​ ¿Cómo se cargan los datos en Data Prep de GCP?

Se puede cargar un archivo .csv., .json. o .txt desde la máquina local, desde Google Cloud Storage o desde Google Query.

6\.​ ¿Qué tipos de datos se pueden preparar en Data Prep de GCP?

Dataprep reconoce los tipos de datos fundamentales String, Integer, Decimal, Boolean, Date/Time, ip address y url. 
También reconoce archivos json, array, y tipos de datos “inteligentes” como ser email, Número de Tarjeta de Crédito, Social Security Number,
Género, ZIP Code.

7\.​ ¿Qué pasos se pueden seguir para limpiar y transformar datos en Data Prep de GCP?

Para limpiar y transformar datos se crea un flujo al cual uno le asocia uno o mas datasets.
En el flujo uno define "recetas" a aplicar en el o los datasets.

8\.​ ¿Cómo se pueden automatizar tareas de preparación de datos en Data Prep de GCP?

Sí, es posible automatizar y programar trabajos de Dataprep utilizando Dataflow. Dataflow puede integrarse con Dataprep para ejecutar tareas de preparación de datos a gran escala y de forma automatizada. 
Esto permite transformar y limpiar datos utilizando la interfaz
visual de Dataprep y luego ejecutar esas transformaciones a través de la infraestructura de
Dataflow.

9\.​ ¿Qué tipos de visualizaciones se pueden crear en Data Prep de GCP?

Con dataprep se pueden realizar gráficos de barra, de línea, de torta, dispersión, histogramas, gráficos de área, graf́icos heatmap, gráficos de caja y otros gráficos especializados.

10\.​¿Cómo se puede garantizar la calidad de los datos en Data Prep de GCP?

Al abrir la tabla del dataset, Cloud Dataprep perfilará automáticamente los contenidos del conjunto de datos y generará histogramas de columnas, además de indicadores de calidad de los datos. Esta información de perfil puede usarse para guiar el proceso de preparación de los datos.

## Google Skills Boost

![login.png](./images/login.png)

![dataprep-succeed.png](./images/dataprep-succeed.png)

![tables-bigquery.png](./images/tables-bigquery.png)

# Ejercicio 4 - Arquitectura


![arquitectura.png](./images/arquitectura.png)