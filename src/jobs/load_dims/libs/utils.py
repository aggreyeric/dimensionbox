import pyspark
from delta import *


   
def getspark():
    builder = pyspark.sql.SparkSession.builder.appName("loadData") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.ui.port", "8080")

    return configure_spark_with_delta_pip(builder).getOrCreate()



def return_table_view(spark, table_name):
    data = spark.read.table(table_name)
    data.createOrReplaceTempView("v_"+ table_name)
    

# with stg_address as (
#     select *
#     from {{ ref('address') }}
# ),

# stg_stateprovince as (
#     select *
#     from {{ ref('stateprovince') }}
# ),

# stg_countryregion as (
#     select *
#     from {{ ref('countryregion') }}
# )