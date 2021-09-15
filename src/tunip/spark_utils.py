from pyspark.sql import SparkSession

from tunip.singleton import Singleton


class SparkConnector(Singleton):

    @property
    def session(self):
        active_session = SparkSession.getActiveSession()
        if active_session:
            return active_session
        else:
            return self.getOrCreate()


    def getOrCreate(self, local=False):
        spark = SparkSession.builder.master("local") if not local else SparkSession.builder
        spark = spark.config("spark.driver.maxResultSize", "8g") \
            .config("spark.sql.broadcastTimeout", "720000") \
            .config("spark.rpc.lookupTimeout", "600s") \
            .config("spark.network.timeout", "600s") \
            .config("mapred.output.compress", "false") \
            .getOrCreate()

        spark.sparkContext._jsc.hadoopConfiguration().set(
            "fs.defaultFS",
            "hdfs://dev01.ascent.com:8020/user/nauts"  # TODO from config
        )

        return spark


spark_conn = SparkConnector()
