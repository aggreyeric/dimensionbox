from libs.utils import getspark, return_table_view,BASE_LAKE_PATH
from pyspark.sql.functions import concat_ws, col, monotonically_increasing_id

spark = getspark()
logger = Log4j(spark)




person_DF = return_table_view(spark, table_name="person", base_path=BASE_LAKE_PATH)
customer_DF = return_table_view(spark, table_name="customer", base_path=BASE_LAKE_PATH)
store_DF = return_table_view(spark, table_name="store", base_path=BASE_LAKE_PATH)


person_DF = person_DF.select(concat_ws(" ",person_DF.firstname, person_DF.lastname).alias("fullname"), person_DF.businessentityid)

customer_DF = customer_DF.select(  customer_DF.customerid,customer_DF.personid, customer_DF.storeid )

store_DF = store_DF.select( (store_DF.businessentityid).alias("storebusinessentityid") , store_DF.storename)






customers_dim = customer_DF.join(person_DF, customer_DF.personid == person_DF.businessentityid).join(
    store_DF, customer_DF.storeid == store_DF.storebusinessentityid).select(
        col("customerid"),col("businessentityid"),col("storename"),col("fullname"),col("storebusinessentityid")
    ).withColumn("customer_key", monotonically_increasing_id())




customers_dim.write.format('delta').mode("overwrite").saveAsTable("dim_customers")



