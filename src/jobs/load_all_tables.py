from delta import *
from utils.funcs import getext, getspark, RAW_DATA_PATH, BASE_LAKE_PATH
from utils.logging import Log4j
import os





spark = getspark()
spark.sparkContext.setLogLevel("WARN")
logger =  Log4j(spark)


list_paths = os.listdir(RAW_DATA_PATH)

for path_ in list_paths:
    file_info = getext(path_)
    try:
        data = spark.read.format(file_info[1]).option("header", "true").load(os.path.join(RAW_DATA_PATH, path_))
        data.show()
        data.write.format("delta").mode("overwrite").save(os.path.join(BASE_LAKE_PATH, file_info[0]))
        logger.info(f"Table {file_info[0]} was loaded succesfully")
        
    except:
        logger.error(f"Table {file_info[0]} was not loaded")
        # raise(f"Error Occured in loading {path_}")