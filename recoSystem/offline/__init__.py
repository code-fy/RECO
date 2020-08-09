import findspark
findspark.init('/usr/local/spark-2.1.1-bin-hadoop2.7', edit_rc=True)
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os



class SparkSessionBase(object):

    SPARK_APP_NAME = None
    SPARK_URL = "spark://hydeMacBook-Pro.lan:7077"

    SPARK_EXECUTOR_MEMORY = "2g"
    SPARK_EXECUTOR_CORES = 2
    SPARK_EXECUTOR_INSTANCES = 2

    ENABLE_HIVE_SUPPORT = False

    def _create_spark_session(self):

        conf = SparkConf()

        config = (
            ("spark.app.name", self.SPARK_APP_NAME),
            ("spark.master", self.SPARK_URL),
            ("spark.executor.memory", self.SPARK_EXECUTOR_MEMORY),
            ("spark.executor.cores", self.SPARK_EXECUTOR_CORES),
            ("spark.executor.instance", self.SPARK_EXECUTOR_INSTANCES)
        )
        conf.setAll(config)

        if self.ENABLE_HIVE_SUPPORT:
            return SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
        else:
            return SparkSession.builder.config(conf=conf).getOrCreate()

