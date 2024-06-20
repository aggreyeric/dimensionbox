import pyspark
from delta import *
import os
BASE_LAKE_PATH = r"C:\Users\Jengo\Desktop\notes_md\Data_Engineering\projectsproto\end_end_projects\dimensionbox\spark-warehouse"

   
def getspark():
    builder = pyspark.sql.SparkSession.builder.appName("loadData") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.ui.port", "8080")

    return configure_spark_with_delta_pip(builder).getOrCreate()



def return_table_view(spark,table_name, base_path=BASE_LAKE_PATH):
    path_ = os.path.join(base_path, table_name)
    data = spark.read.format("delta").load(path_)
    return data
  
    

