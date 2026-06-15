# pip install h3==4.3.0
from h3.api import basic_str as h3
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("teste").getOrCreate()

data = [
    (-23.5505, -46.6333),  # São Paulo
    (-23.5510, -46.6340),
    (-22.9068, -43.1729),  # Rio
    (-22.9070, -43.1730),
    (-30.0346, -51.2177),  # Porto Alegre
]
df = spark.createDataFrame(data, ["latitude", "longitude"])

def latlon_to_h3(lat, lon, resolution=8):
    return h3.latlng_to_cell(lat, lon, resolution)

h3_udf = udf(latlon_to_h3, StringType())

df_h3 = df.withColumn("h3_index", h3_udf("latitude", "longitude"))
df_h3.show(truncate=False)

------------------------------------------------------------------------------
# pip install folium==0.20.0
import folium
from h3.api import basic_str as h3
import json

points = [
    (-23.5505, -46.6333),  # SP
    (-23.5510, -46.6340),  # SP
    (-22.9068, -43.1729),  # RJ
    (-22.9070, -43.1730),  # RJ
    (-30.0346, -51.2177),  # POA
]

# calcular hexágonos
resolution = 10
h3_indices = [h3.latlng_to_cell(lat, lon, resolution) for lat, lon in points]
h3_indices = list(set(h3_indices))  # remover duplicados

# iniciar mapa centralizado em SP
m = folium.Map(location=[-23.5505, -46.6333], zoom_start=5)

# desenhar hexágonos
for h in h3_indices:
    boundary = h3.cell_to_boundary(h)
    folium.Polygon(
        locations=boundary,
        color="blue",
        fill=True,
        fill_opacity=0.3,
        popup=h
    ).add_to(m)

# marcar os pontos também
for lat, lon in points:
    folium.CircleMarker(location=[lat, lon], radius=5, color="red").add_to(m)

m.save("mapa_hexagonos10.html")
print("Arquivo gerado: mapa_hexagonos10.html")

------------------------------------------------------------------------------
# pip install pygeohash==3.1.3
import pygeohash as pgh
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("teste").getOrCreate()

# função para transformar lat/lon em geohash
def to_geohash(lat, lon, precision=7):
    return pgh.encode(lat, lon, precision=precision)

geohash_udf = udf(to_geohash, StringType())

# dataframe de exemplo
data = [
    (-23.5505, -46.6333),  # São Paulo
    (-23.5510, -46.6340),
    (-22.9068, -43.1729),  # Rio
    (-22.9070, -43.1730),
    (-30.0346, -51.2177),  # Porto Alegre
]
df = spark.createDataFrame(data, ["latitude", "longitude"])

# aplicando o geohash
df_geo = df.withColumn("geohash", geohash_udf("latitude", "longitude"))
df_geo.show(truncate=False)

------------------------------------------------------------------------------
# pip install shapely==2.1.1 apache-sedona==1.5.1 pygeohash==3.1.3
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from pyspark.sql.functions import expr

spark = (
    SparkSession.builder
    .appName("sedona-geofence")
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-python-adapter-3.0_2.12:1.4.1,org.datasyslab:geotools-wrapper:1.5.1-28.2"
    )
    .getOrCreate()
)

# registrar funções do Sedona no SparkSQL
SedonaRegistrator.registerAll(spark)

data = [
    (-23.542204, -46.576563, "2024-06-01", "12345"),
    (-23.543300, -46.576570, "2024-06-01", "12345"),
    (-23.528857, -46.566307, "2024-06-01", "67890"),
]

columns = ["latitude", "longitude", "data_evento", "documento"]

df = spark.createDataFrame(data, columns)

# transforma em ponto Sedona
df_geom = df.withColumn(
    "point",
    expr("ST_Point(cast(longitude as Decimal(24,20)), cast(latitude as Decimal(24,20)))")
)
df_geom.show(truncate=False)

# Sedona espera graus → transforme metros para graus aproximadamente:
raio_graus = 0.001  # ~100m aproximado

df_geofence = df_geom.withColumn(
    "geofence",
    expr(f"ST_Buffer(point, {raio_graus})")
)

df_geofence.select("documento", "geofence").show(truncate=False)

novo_ponto_lat = -23.542204
novo_ponto_lon = -46.576563

novo_ponto_wkt = f"POINT({novo_ponto_lon} {novo_ponto_lat})"

df_novo_ponto = spark.createDataFrame([(novo_ponto_wkt,)], ["wkt"]) \
    .withColumn("geometry", expr("ST_GeomFromWKT(wkt)"))

df_novo_ponto.show(truncate=False)

joined = df_geofence.join(
    df_novo_ponto,
    expr("ST_Contains(geofence, geometry)")
)

joined.show(truncate=False)

