import os
from libs.utils import getspark, return_table_view, BASE_LAKE_PATH
from libs.logging import Log4j
from pyspark.sql.functions import monotonically_increasing_id


spark = getspark()
logger = Log4j(spark)


date_DF = return_table_view(spark, table_name="date", base_path=BASE_LAKE_PATH)

date_DF.show()





date_dim = date_DF.withColumn("date_key", monotonically_increasing_id())



date_dim.write.format('delta').mode("overwrite").save(os.path.join(BASE_LAKE_PATH,"dim_date" ))



