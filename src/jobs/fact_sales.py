
import os
import hashlib
from utils.funcs import getspark, return_table_view,BASE_LAKE_PATH
from utils.logging import Log4j
from pyspark.sql.functions import  col, concat, udf


spark = getspark()
logger = Log4j(spark)
hash_sha256_udf = udf(lambda x: hashlib.sha256(x.encode()).hexdigest())

# Read Tables 
salesorderheader_DF = return_table_view(spark, table_name="salesorderheader", base_path=BASE_LAKE_PATH).withColumnRenamed("salesorderid", "salesorderid_FK")
salesorderdetail_DF = return_table_view(spark, table_name="salesorderdetail", base_path=BASE_LAKE_PATH)

# Create fact table
fact_sales = (
    salesorderdetail_DF.
    join(salesorderheader_DF, salesorderheader_DF.salesorderid_FK == salesorderdetail_DF.salesorderid, how="inner")
    .withColumn("sales_key", hash_sha256_udf(concat("salesorderid","salesorderdetailid")))
    .select("sales_key",hash_sha256_udf("customerid"), hash_sha256_udf("shiptoaddressid"), hash_sha256_udf("orderdate"),"salesorderid","salesorderdetailid","unitprice","orderqty" ).withColumn("revenue", col("unitprice") * col("orderqty"))
    )


# Write Table to lakehouse
fact_sales.write.format('parquet').mode("overwrite").save(os.path.join(BASE_LAKE_PATH,"fact_sales" ))



