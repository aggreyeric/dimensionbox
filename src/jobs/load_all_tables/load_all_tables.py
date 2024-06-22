from delta import *
from libs.utils import getext, getspark
from libs.logging import Log4j
import os





spark = getspark()
logger =  Log4j(spark)

BASE_PATH = r"C:\Users\Ubong\Desktop\spark_stuff\project\dimensionbox\src\data\raw"
list_paths = os.listdir(BASE_PATH)

for path_ in list_paths:
    file_info = getext(path_)
    try:
        data = spark.read.format(file_info[1]).option("header", "true").load(os.path.join(BASE_PATH, path_))
        data.write.format("delta").mode("overwrite").saveAsTable(file_info[0])
        logger.info(f"Table {file_info[0]} was loaded succesfully")
        
    except:
        logger.error(f"Table {file_info[0]} was not loaded")
        # raise(f"Error Occured in loading {path_}")