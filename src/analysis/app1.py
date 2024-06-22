from reuse import getspark, LAKE_URL
from os import path

spark = getspark() 

df = spark.read.format("delta").option("header", True).load(path.join(LAKE_URL, "dim_customers"))

df.show()