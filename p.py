
# Baixar e instalar dependências
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://archive.apache.org/dist/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz
!tar xf spark-3.2.4-bin-hadoop3.2.tgz
!pip install -q findspark


# Configuração
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.2.4-bin-hadoop3.2"

import findspark
findspark.init()

# Imports e init
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce
from operator import or_

spark = SparkSession.builder \
    .master('local[*]') \
    .appName('Iniciando com Spark') \
    .getOrCreate()

# Novos Dados
data = [
    ("12345678900","2023-11-22",0,3,10,0,0,2,3),
    ("12345678900","2023-11-21",0,3,10,0,0,2,2),
    ("98765432100","2023-11-15",0,0,0,0,0,1,2),
    # ("12345678900",None,0,1,8,0,2,2,1),
    # ("98765432100",None,1,6,8,0,1,1,1),
    # ("246810","2023-11-22",0,0,0,0,0,0,2),
    # # ("13579",None,0,0,4,2,2,3,1),
    ("246810","2023-11-10",0,0,0,0,0,0,1)
  ]

schema = StructType([ \
    StructField("documento",StringType(),True), \
    StructField("data_a",StringType(),True), \
    StructField("fraude_a_3",IntegerType(),True), \
    StructField("fraude_a_6", IntegerType(), True), \
    StructField("fraude_a_9", IntegerType(), True), \
    StructField("fraude_b_3", IntegerType(), True), \
    StructField("fraude_b_6", IntegerType(), True), \
    StructField("fraude_b_9", IntegerType(), True), \
    StructField("registro", IntegerType(), True) \
  ])

# data = [
#     {
#         "documento": "189513218",
#         "data_o": "2022-11-22",
#         "owner_3": 0,
#         "owner_6": 3,
#         "owner_9": 4
#     },
#     {
#         "documento": "189513218",
#         "data_o": "2022-05-16",
#         "owner_3": 6,
#         "owner_6": 2,
#         "owner_9": 9
#     },
#     {
#         "documento": "189513218",
#         "data_o": "1970-00-00",
#         "owner_3": 0,
#         "owner_6": 0,
#         "owner_9": 0
#     },
#     {
#         "documento": "65789332",
#         "data_o": "2022-09-13",
#         "owner_3": 4,
#         "owner_6": 0,
#         "owner_9": 7
#     },
#     {
#         "documento": "55555555",
#         "data_o": "1970-00-00",
#         "owner_3": 0,
#         "owner_6": 0,
#         "owner_9": 0
#     },
#     {
#         "documento": "12345678900",
#         "data_a": "2023-11-22",
#         "fraude_a_3": 0,
#         "fraude_a_6": 3,
#         "fraude_a_9": 9,
#         "registro": 3
#     },
#     {
#         "documento": "12345678900",
#         "data_a": "2023-11-21",
#         "fraude_a_3": 0,
#         "fraude_a_6": 3,
#         "fraude_a_9": 10,
#         "registro": 2
#     },
#     {
#         "documento": "98765432100",
#         "data_a": "2023-11-15",
#         "fraude_a_3": 12,
#         "fraude_a_6": 5,
#         "fraude_a_9": 7,
#         "registro": 2
#     },
#     {
#         "documento": "12345678900",
#         "fraude_a_3": 0,
#         "fraude_a_6": 1,
#         "fraude_a_9": 8,
#         "registro": 1
#     },
#     {
#         "documento": "98765432100",
#         "fraude_a_3": 1,
#         "fraude_a_6": 6,
#         "fraude_a_9": 8,
#         "registro": 1
#     },
#     {
#         "documento": "246810",
#         "data_a": "2023-11-22",
#         "fraude_a_3": 0,
#         "fraude_a_6": 0,
#         "fraude_a_9": 0,
#         "registro": 2
#     },
#     {
#         "documento": "246810",
#         "data_a": "2023-11-10",
#         "fraude_a_3": 0,
#         "fraude_a_6": 0,
#         "fraude_a_9": 0,
#         "registro": 1
#     }
# ]

# schema = StructType([ \
#     StructField("documento",StringType(),True), \
#     StructField("data_o",StringType(),True), \
#     StructField("owner_3",IntegerType(),True), \
#     StructField("owner_6",IntegerType(),True), \
#     StructField("owner_9",IntegerType(),True), \
#     StructField("data_a",StringType(),True), \
#     StructField("fraude_a_3",IntegerType(),True), \
#     StructField("fraude_a_6", IntegerType(), True), \
#     StructField("fraude_a_9", IntegerType(), True), \
#     StructField("registro", IntegerType(), True) \
#   ])

df = spark.createDataFrame(data=data,schema=schema)

df = df.filter((df.fraude_a_3 > 0)
  | (df.fraude_a_6 > 0)
  | (df.fraude_a_9 > 0)
  | (df.fraude_b_3 > 0)
  | (df.fraude_b_6 > 0)
  | (df.fraude_b_9 > 0)
)

# df = df.dropna(subset="documento")
df = df.withColumn("data_a", F.when(F.col("data_a") <= "1970-00-00", None).otherwise(F.col("data_a")))
df.show(truncate=False)

window_spec = Window.partitionBy("documento")
df = (df
    # .withColumn("max_owner_3", F.max("owner_3").over(window_spec))
    # .withColumn("max_owner_6", F.max("owner_6").over(window_spec))
    # .withColumn("max_owner_9", F.max("owner_9").over(window_spec))

    # .withColumn("max_data_o", F.max("data_o").over(window_spec))
    # .withColumn("min_data_o", F.min("data_o").over(window_spec))

    # .withColumn("is_max_owner_3", F.col("owner_3") == F.col("max_owner_3"))
    # .withColumn("is_max_owner_6", F.col("owner_6") == F.col("max_owner_6"))
    # .withColumn("is_max_owner_9", F.col("owner_9") == F.col("max_owner_9"))

    # .withColumn("is_max_data_o", F.col("data_o") == F.col("max_data_o"))

    # .withColumn("data_when_max_owner_3", F.when(F.col("is_max_owner_3"), F.col("data_o")))
    # .withColumn("data_when_max_owner_6", F.when(F.col("is_max_owner_6"), F.col("data_o")))
    # .withColumn("data_when_max_owner_9", F.when(F.col("is_max_owner_9"), F.col("data_o")))

    # .withColumn("owner_3_when_max_data", F.when(F.col("is_max_data_o"), F.col("owner_3")))
    # .withColumn("owner_6_when_max_data", F.when(F.col("is_max_data_o"), F.col("owner_6")))
    # .withColumn("owner_9_when_max_data", F.when(F.col("is_max_data_o"), F.col("owner_9")))

    ##

    .withColumn("max_fraude_a_3", F.max("fraude_a_3").over(window_spec))
    .withColumn("max_fraude_a_6", F.max("fraude_a_6").over(window_spec))
    .withColumn("max_fraude_a_9", F.max("fraude_a_9").over(window_spec))
    .withColumn("max_fraude_b_3", F.max("fraude_b_3").over(window_spec))
    .withColumn("max_fraude_b_6", F.max("fraude_b_6").over(window_spec))
    .withColumn("max_fraude_b_9", F.max("fraude_b_9").over(window_spec))

    .withColumn("max_data_a", F.max("data_a").over(window_spec))
    .withColumn("min_data_a", F.min("data_a").over(window_spec))

    .withColumn("is_max_fraude_a_3", F.col("fraude_a_3") == F.col("max_fraude_a_3"))
    .withColumn("is_max_fraude_a_6", F.col("fraude_a_6") == F.col("max_fraude_a_6"))
    .withColumn("is_max_fraude_a_9", F.col("fraude_a_9") == F.col("max_fraude_a_9"))
    .withColumn("is_max_fraude_b_3", F.col("fraude_b_3") == F.col("max_fraude_b_3"))
    .withColumn("is_max_fraude_b_6", F.col("fraude_b_6") == F.col("max_fraude_b_6"))
    .withColumn("is_max_fraude_b_9", F.col("fraude_b_9") == F.col("max_fraude_b_9"))

    .withColumn("is_max_data_a", F.col("data_a") == F.col("max_data_a"))

    .withColumn("data_when_max_fraude_a_3", F.when(F.col("is_max_fraude_a_3"), F.col("data_a")))
    .withColumn("data_when_max_fraude_a_6", F.when(F.col("is_max_fraude_a_6"), F.col("data_a")))
    .withColumn("data_when_max_fraude_a_9", F.when(F.col("is_max_fraude_a_9"), F.col("data_a")))
    .withColumn("data_when_max_fraude_b_3", F.when(F.col("is_max_fraude_b_3"), F.col("data_a")))
    .withColumn("data_when_max_fraude_b_6", F.when(F.col("is_max_fraude_b_6"), F.col("data_a")))
    .withColumn("data_when_max_fraude_b_9", F.when(F.col("is_max_fraude_b_9"), F.col("data_a")))

    .withColumn("fraude_a_3_when_max_data", F.when(F.col("is_max_data_a"), F.col("fraude_a_3")))
    .withColumn("fraude_a_6_when_max_data", F.when(F.col("is_max_data_a"), F.col("fraude_a_6")))
    .withColumn("fraude_a_9_when_max_data", F.when(F.col("is_max_data_a"), F.col("fraude_a_9")))
    .withColumn("fraude_b_3_when_max_data", F.when(F.col("is_max_data_a"), F.col("fraude_b_3")))
    .withColumn("fraude_b_6_when_max_data", F.when(F.col("is_max_data_a"), F.col("fraude_b_6")))
    .withColumn("fraude_b_9_when_max_data", F.when(F.col("is_max_data_a"), F.col("fraude_b_9")))

)
df.show()

def compare_and_get_max(*cols):
    acc = F.col(cols[0])
    for col in cols[1:]:
        acc = F.when(F.col(col) > acc, F.col(col)).otherwise(acc)
    return acc

ex6 = [
    "max_owner_3",
    "max_owner_6",
    "max_owner_9"
]

ex7a = [
    "max_fraude_a_3",
    "max_fraude_a_6",
    "max_fraude_a_9"
]
ex7b = [
    "max_fraude_b_3",
    "max_fraude_b_6",
    "max_fraude_b_9"
]

df = (
    df.groupBy("documento")
    .agg(
        *[
            F.max(column).alias(column)
            for column in (
                # "max_owner_3",
                # "max_owner_6",
                # "max_owner_9",
                # "max_data_o",
                # "min_data_o",
                # "data_when_max_owner_3",
                # "data_when_max_owner_6",
                # "data_when_max_owner_9",
                # "owner_3_when_max_data",
                # "owner_6_when_max_data",
                # "owner_9_when_max_data",

                "max_fraude_a_3",
                "max_fraude_a_6",
                "max_fraude_a_9",
                "max_fraude_b_3",
                "max_fraude_b_6",
                "max_fraude_b_9",
                "max_data_a",
                "min_data_a",
                "data_when_max_fraude_a_3",
                "data_when_max_fraude_a_6",
                "data_when_max_fraude_a_9",
                "data_when_max_fraude_b_3",
                "data_when_max_fraude_b_6",
                "data_when_max_fraude_b_9",
                "fraude_a_3_when_max_data",
                "fraude_a_6_when_max_data",
                "fraude_a_9_when_max_data",
                "fraude_b_3_when_max_data",
                "fraude_b_6_when_max_data",
                "fraude_b_9_when_max_data",
                "registro"
            )
        ],
    )
    # .withColumn("max_owner", compare_and_get_max("max_owner_3", "max_owner_6", "max_owner_9"))
    # .withColumn("max_data_owner", reduce(
    #     lambda acc, nome: acc.when((F.col("max_owner") == F.col(nome)), F.col(f"data_when_{nome}")), ex6, F
    # ))
    .withColumn("max_fraude_a", compare_and_get_max("max_fraude_a_3", "max_fraude_a_6", "max_fraude_a_9"))
    .withColumn("max_data_fraude_a", F.when(F.col("max_fraude_a") == 0, None).otherwise(
        reduce(
          lambda acc, nome: acc.when((F.col("max_fraude_a") == F.col(nome)), F.col(f"data_when_{nome}")), ex7a, F
      )
    ))
    .withColumn("max_fraude_b", compare_and_get_max("max_fraude_b_3", "max_fraude_b_6", "max_fraude_b_9"))
    .withColumn("max_data_fraude_b", reduce(
        lambda acc, nome: acc.when((F.col("max_fraude_b") == F.col(nome)), F.col(f"data_when_{nome}")), ex7b, F
    ))
)
drops = (
    "data_when_max_fraude_a_3",
    "data_when_max_fraude_a_6",
    "data_when_max_fraude_a_9",
    "data_when_max_fraude_b_3",
    "data_when_max_fraude_b_6",
    "data_when_max_fraude_b_9",
)
df = df.drop(*drops)

df.show()


data2 = [
    ("12345678900", "2023-11-01", "2022-03-16", 1, 2, 9, 0, 0, 0, "2022-07-01", 12, 1, 12, 9, None, 0, 0, 0, 0, 100),
    ("98765432100", "2023-11-01", "2023-05-29", 0, 0, 0, 0, 0, 0, None, 0, 0, 0, 0, "2022-12-12", 3, 3, 0, 0, 54),
    ("4455667788", "2023-10-10", "2023-09-09", 1, 2, 3, 4, 5, 6, "2023-10-10", 3, 1, 2, 3, "2023-10-10", 6, 4, 5, 6, 80)
]


schema2 = StructType([ \
    StructField("documento",StringType(),True), \
    StructField("max_data_a",StringType(),True), \
    StructField("min_data_a",StringType(),True), \
    StructField("fraude_a_3_when_max_data", IntegerType(),True), \
    StructField("fraude_a_6_when_max_data", IntegerType(), True), \
    StructField("fraude_a_9_when_max_data", IntegerType(), True), \
    StructField("fraude_b_3_when_max_data", IntegerType(), True), \
    StructField("fraude_b_6_when_max_data", IntegerType(), True), \
    StructField("fraude_b_9_when_max_data", IntegerType(), True), \
    StructField("max_data_fraude_a",StringType(),True), \
    StructField("max_fraude_a",IntegerType(),True), \
    StructField("max_fraude_a_3",IntegerType(),True), \
    StructField("max_fraude_a_6", IntegerType(), True), \
    StructField("max_fraude_a_9", IntegerType(), True), \
    StructField("max_data_fraude_b",StringType(),True), \
    StructField("max_fraude_b", IntegerType(), True), \
    StructField("max_fraude_b_3", IntegerType(), True), \
    StructField("max_fraude_b_6", IntegerType(), True), \
    StructField("max_fraude_b_9", IntegerType(), True), \
    StructField("registro", IntegerType(), True) \
])

df2 = spark.createDataFrame(data2,schema=schema2)

df2.show()


df_union = df.unionByName(df2)

df_union.printSchema()
df_union.show()


df_union = (df_union
  .withColumn("max_fraude_a_3_union", F.max("max_fraude_a_3").over(window_spec))
  .withColumn("max_fraude_a_6_union", F.max("max_fraude_a_6").over(window_spec))
  .withColumn("max_fraude_a_9_union", F.max("max_fraude_a_9").over(window_spec))
  .withColumn("max_fraude_b_3_union", F.max("max_fraude_b_3").over(window_spec))
  .withColumn("max_fraude_b_6_union", F.max("max_fraude_b_6").over(window_spec))
  .withColumn("max_fraude_b_9_union", F.max("max_fraude_b_9").over(window_spec))

  .withColumn("max_data_a_union", F.max("max_data_a").over(window_spec))
  .withColumn("min_data_a_union", F.min("min_data_a").over(window_spec))

  .withColumn("is_max_fraude_a_3", F.col("max_fraude_a_3") == F.col("max_fraude_a_3_union"))
  .withColumn("is_max_fraude_a_6", F.col("max_fraude_a_6") == F.col("max_fraude_a_6_union"))
  .withColumn("is_max_fraude_a_9", F.col("max_fraude_a_9") == F.col("max_fraude_a_9_union"))
  .withColumn("is_max_fraude_b_3", F.col("max_fraude_b_3") == F.col("max_fraude_b_3_union"))
  .withColumn("is_max_fraude_b_6", F.col("max_fraude_b_6") == F.col("max_fraude_b_6_union"))
  .withColumn("is_max_fraude_b_9", F.col("max_fraude_b_9") == F.col("max_fraude_b_9_union"))

  .withColumn("is_max_data_a", F.col("max_data_a") == F.col("max_data_a_union"))

  .withColumn("data_when_max_fraude_a_3", F.when(F.col("is_max_fraude_a_3"), F.col("max_data_fraude_a")))
  .withColumn("data_when_max_fraude_a_6", F.when(F.col("is_max_fraude_a_6"), F.col("max_data_fraude_a")))
  .withColumn("data_when_max_fraude_a_9", F.when(F.col("is_max_fraude_a_9"), F.col("max_data_fraude_a")))
  .withColumn("data_when_max_fraude_b_3", F.when(F.col("is_max_fraude_b_3"), F.col("max_data_fraude_a")))
  .withColumn("data_when_max_fraude_b_6", F.when(F.col("is_max_fraude_b_6"), F.col("max_data_fraude_a")))
  .withColumn("data_when_max_fraude_b_9", F.when(F.col("is_max_fraude_b_9"), F.col("max_data_fraude_a")))

  .withColumn("fraude_a_3_when_max_data_union", F.when(F.col("is_max_data_a"), F.col("fraude_a_3_when_max_data")))
  .withColumn("fraude_a_6_when_max_data_union", F.when(F.col("is_max_data_a"), F.col("fraude_a_6_when_max_data")))
  .withColumn("fraude_a_9_when_max_data_union", F.when(F.col("is_max_data_a"), F.col("fraude_a_9_when_max_data")))
  .withColumn("fraude_b_3_when_max_data_union", F.when(F.col("is_max_data_a"), F.col("fraude_b_3_when_max_data")))
  .withColumn("fraude_b_6_when_max_data_union", F.when(F.col("is_max_data_a"), F.col("fraude_b_6_when_max_data")))
  .withColumn("fraude_b_9_when_max_data_union", F.when(F.col("is_max_data_a"), F.col("fraude_b_9_when_max_data")))
)

df_union.show()


df_union = (
    df_union.groupBy("documento")
    .agg(
        *[
            F.max(column).alias(column)
            for column in (
                "max_fraude_a_3_union",
                "max_fraude_a_6_union",
                "max_fraude_a_9_union",
                "max_fraude_b_3_union",
                "max_fraude_b_6_union",
                "max_fraude_b_9_union",
                "max_data_a_union",
                "min_data_a_union",
                "max_data_fraude_a",
                "max_data_fraude_b",
                "data_when_max_fraude_a_3",
                "data_when_max_fraude_a_6",
                "data_when_max_fraude_a_9",
                "data_when_max_fraude_b_3",
                "data_when_max_fraude_b_6",
                "data_when_max_fraude_b_9",
                "fraude_a_3_when_max_data_union",
                "fraude_a_6_when_max_data_union",
                "fraude_a_9_when_max_data_union",
                "fraude_b_3_when_max_data_union",
                "fraude_b_6_when_max_data_union",
                "fraude_b_9_when_max_data_union",
                "registro"
            )
        ],
    )
    .withColumn("max_fraude_a_union", compare_and_get_max("max_fraude_a_3_union", "max_fraude_a_6_union", "max_fraude_a_9_union"))
    .withColumn("max_data_fraude_a_union", F.when(F.col("max_fraude_a_union") == 0, None).otherwise(
        reduce(
          lambda acc, nome: acc.when((F.col("max_fraude_a_union") == F.col(f"{nome}_union")), F.col(f"max_data_fraude_a")), ex7a, F
      )
    ))
    .withColumn("max_fraude_b_union", compare_and_get_max("max_fraude_b_3_union", "max_fraude_b_6_union", "max_fraude_b_9_union"))
    .withColumn("max_data_fraude_b_union", F.when(F.col("max_fraude_b_union") == 0, None).otherwise(
      reduce(
        lambda acc, nome: acc.when((F.col("max_fraude_b_union") == F.col(f"{nome}_union")), F.col(f"max_data_fraude_b")), ex7b, F
      )
    ))
)
drops = (
    "data_when_max_fraude_a_3",
    "data_when_max_fraude_a_6",
    "data_when_max_fraude_a_9",
    "data_when_max_fraude_b_3",
    "data_when_max_fraude_b_6",
    "data_when_max_fraude_b_9",
)
df_union = df_union.drop(*drops)

# columns_to_rename = df_union.columns

# for column in columns_to_rename:
#     new_column_name = column.replace("_union", "")
#     df_union = df_union.withColumnRenamed(column, new_column_name)

df_union.show()
