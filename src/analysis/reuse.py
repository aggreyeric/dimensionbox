import pyspark
from delta import *

def getspark():
    builder = pyspark.sql.SparkSession.builder.appName("loadData") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.ui.port", "8080")

    return configure_spark_with_delta_pip(builder).getOrCreate()



LAKE_URL = r"C:\Users\Jengo\Desktop\notes_md\Data_Engineering\projectsproto\end_end_projects\dimensionbox\spark-warehouse"