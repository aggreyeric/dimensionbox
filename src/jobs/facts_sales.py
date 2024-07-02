
import os
from libs.logging import Log4j
from libs.utils import getspark, return_table_view,BASE_LAKE_PATH,hash_sha256_udf
from pyspark.sql.functions import  col, concat


spark = getspark()
logger = Log4j(spark)


salesorderheader_DF = return_table_view(spark, table_name="salesorderheader", base_path=BASE_LAKE_PATH).withColumnRenamed("salesorderid", "salesorderid_FK")
salesorderdetail_DF = return_table_view(spark, table_name="salesorderdetail", base_path=BASE_LAKE_PATH)





fact_sales = salesorderdetail_DF.join(salesorderheader_DF, salesorderheader_DF.salesorderid_FK == salesorderdetail_DF.salesorderid, how="inner").withColumn("sales_key", hash_sha256_udf(concat("salesorderid","salesorderdetailid"))).select("sales_key",hash_sha256_udf("customerid"), hash_sha256_udf("shiptoaddressid"), hash_sha256_udf("orderdate"),"salesorderid","salesorderdetailid","unitprice","orderqty" ).withColumn("revenue", col("unitprice") * col("orderqty"))

fact_sales.show()

fact_sales.printSchema()


fact_sales.write.format('delta').mode("overwrite").save(os.path.join(BASE_LAKE_PATH,"fact_sales" ))



