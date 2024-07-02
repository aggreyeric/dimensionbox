import os
from libs.utils import getspark, return_table_view, BASE_LAKE_PATH, hash_sha256_udf, hash_sha256
from libs.logging import Log4j
from pyspark.sql.functions import monotonically_increasing_id, col
from pyspark.sql.types import StringType


spark = getspark()
logger = Log4j(spark)


date_DF = return_table_view(spark, table_name="date", base_path=BASE_LAKE_PATH)

# date_DF.show()





# date_dim = date_DF.withColumn("date_key", hash_sha256_udf(col("date_day")))
spark.udf.register("hash_sha256_udf", hash_sha256,StringType())
date_DF.createOrReplaceTempView("dim_datest")

date_dim = spark.sql("""
                     select hash_sha256_udf(date_day) as date_key, * from dim_datest
                     
                     """)




date_dim.printSchema()



date_dim.write.format('delta').mode("overwrite").save(os.path.join(BASE_LAKE_PATH,"dim_date" ))



