from libs.utils import getspark, return_table_view, BASE_LAKE_PATH
from libs.logging import Log4j
from pyspark.sql.functions import monotonically_increasing_id, col
import os


spark = getspark()
logger = Log4j(spark)


address_DF = return_table_view(spark, table_name="address", base_path=BASE_LAKE_PATH)
stateprovince_DF = return_table_view(spark, table_name="stateprovince", base_path=BASE_LAKE_PATH)
stateprovince_DF = stateprovince_DF.withColumnRenamed("name", "state_name")
countryregion_DF = return_table_view(spark, table_name="countryregion",base_path=BASE_LAKE_PATH)
countryregion_DF =countryregion_DF.withColumnRenamed("name", "country_name")



address_dim = address_DF.join(
    stateprovince_DF, address_DF.stateprovinceid == stateprovince_DF.stateprovinceid
).join(
    countryregion_DF,
    stateprovince_DF.countryregioncode == countryregion_DF.countryregioncode,
).withColumn("address_key", monotonically_increasing_id()).select(col("address_key"), col("addressid"),col("city").alias("city_name"), col("state_name"), col("country_name"))


address_dim.write.format('delta').mode("overwrite").save(os.path.join(BASE_LAKE_PATH,"dim_address"))

