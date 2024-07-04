import pyspark
from pyspark.sql.functions import udf
from delta import *
import os
from dotenv import load_dotenv
import re
import hashlib


def hash_sha256(str_to_hash):
    return hashlib.sha256(str_to_hash.encode()).hexdigest()

hash_sha256_udf = udf(lambda x: hash_sha256(x))


load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)),".env.conf"))

BASE_LAKE_PATH= os.getenv('BASE_LAKE_PATH')
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH")
# BASE_LAKE_PATH = r"C:\Users\Ubong\Desktop\spark_stuff\project\dimensionbox\spark-warehouse"
   
def getspark():
    builder = pyspark.sql.SparkSession.builder.appName("loadData") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.ui.port", "8080")

    return configure_spark_with_delta_pip(builder).getOrCreate()



    
def getext(file_name):
    pattern = r'^(.*?)(\.[a-zA-Z0-9]+)$'
    match = re.search(pattern, file_name)
    if match:
        file_name = match.group(1)
        extension = match.group(2)
        return (file_name, extension[1:])
    else:
        return None
    

def return_table_view(spark,table_name, base_path=BASE_LAKE_PATH):
    path_ = os.path.join(base_path, table_name)
    data = spark.read.format("delta").load(path_)
    return data
  

def load_path(table_name):
    return os.path.join(BASE_LAKE_PATH, table_name)
