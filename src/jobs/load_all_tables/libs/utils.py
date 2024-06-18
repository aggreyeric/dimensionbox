import re
import pyspark
from delta import *

def getext(file_name):
    pattern = r'^(.*?)(\.[a-zA-Z0-9]+)$'
    match = re.search(pattern, file_name)
    if match:
        file_name = match.group(1)
        extension = match.group(2)
        return (file_name, extension[1:])
    else:
        return None
    
    
    
def getspark():
    builder = pyspark.sql.SparkSession.builder.appName("loadData") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.ui.port", "8080")

    return configure_spark_with_delta_pip(builder).getOrCreate()