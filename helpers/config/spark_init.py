import os
import sys
import pyspark
import random
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, Row
from pyspark.sql.functions import lit, udf, col, weekofyear, countDistinct, desc
from pyspark.sql.types import DateType, IntegerType, LongType, Row, StructType, StructField, StringType
from pyspark.sql.functions import lit
from pyspark.sql import Window
import pyspark.sql.functions as f
import findspark


def init_spark(application_name: str,
               cors_per_executors: int = 5,
               cores_total: int = 20):
    if not application_name:
        raise Exception('Application name is undefined')
    SPARK_APPLICATION_NAME = application_name
    SPARK_CORES_PER_EXECUTOR = cors_per_executors
    SPARK_CORES_TOTAL = cores_total
    SPARK_MEMORY_EXECUTOR = '10g'
    SPARK_MEMORY_DRIVE = '6g'
    SPARK_MASTER_HOST = '10.109.2.4'
    SPARK_DRIVER_HOST = '10.109.2.4'
    SPARK_PORT = 7077
    SPARK_UI_PORT = 8800

    os.environ['SPARK_HOME'] = '/opt/spark'
    conf = SparkConf()
    conf.setMaster(
        f'spark://{SPARK_MASTER_HOST}:{SPARK_PORT}').setAppName(SPARK_APPLICATION_NAME)
    conf.set('spark.driver.host', SPARK_DRIVER_HOST)
    conf.set('spark.executor.memory', SPARK_MEMORY_EXECUTOR)
    conf.set('spark.driver.memory', SPARK_MEMORY_DRIVE)
    conf.set('spark.executor.cores', SPARK_CORES_PER_EXECUTOR)
    conf.set('spark.cores.max', SPARK_CORES_TOTAL)
    conf.set('spark.ui.port', SPARK_UI_PORT)
    conf.set('spark.jars', '/home/hungnp5/ecommerce-datawarehouse/helpers/driver/mysql-connector-java-8.0.24.jar')
    conf.set('spark.driver.extraClassPath',
             '/home/hungnp5/ecommerce-datawarehouse/helpers/driver/*')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)

    return spark, sqlContext
