from delta import *
import os


import re

def getext(file_name):
    pattern = r'^(.*?)(\.[a-zA-Z0-9]+)$'
    match = re.search(pattern, file_name)
    if match:
        file_name = match.group(1)
        extension = match.group(2)
        return (file_name, extension[1:])
    else:
        return None


# logging = importlib.util.spec_from_file_location("logging", r"libs\logging.py")



import pyspark

builder = pyspark.sql.SparkSession.builder.appName("loadData") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.ui.port", "8080")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# logger =  logging.Log4j(spark)

BASE_PATH = r"C:\Users\Jengo\Desktop\notes_md\Data_Engineering\projectsproto\end_end_projects\dimensionbox\src\data\raw"
list_paths = os.listdir(BASE_PATH)

for path_ in list_paths:
    file_info = getext(path_)
    try:
        data = spark.read.format(file_info[1]).option("header", "true").load(os.path.join(BASE_PATH, path_))
        data.write.format("delta").mode("overwrite").saveAsTable(file_info[0])
        # logger.info(f"Table {file_name} was loaded succesfully")
        
    except:
        # logger.error(f"Table {file_name} was not loaded")
        raise(f"Error Occured in loading {path_}")