from pyspark.sql import SparkSession

from tunip.service_config import get_service_config
from tunip.singleton import Singleton


class SparkConnector(Singleton):

    @property
    def session(self):
        # >= spark 3.0
        # active_session = SparkSession.getActiveSession()
        # if active_session:
        #     return active_session
        # else:
        #     
        return self.getOrCreate()


    def getOrCreate(self, local=False, servers_path=None, force_service_level=None):
        service_config = get_service_config(servers_path, force_service_level)

        if not local:
            spark = SparkSession.builder.master("yarn").config("spark.submit.deployMode", "client")
        else:
            spark = SparkSession.builder.master("local")

        spark = spark.config("spark.driver.maxResultSize", "8g") \
            .config("spark.sql.broadcastTimeout", "720000") \
            .config("spark.rpc.lookupTimeout", "600s") \
            .config("spark.network.timeout", "600s") \
            .getOrCreate()

        spark.sparkContext._jsc.hadoopConfiguration().set(
            "fs.defaultFS",
            f"{service_config.hdfs_prefix}/user/{service_config.username}"
        )

        return spark
    
    def update(self, conf_dict):
        """
        update and reload spark session given configuration input
        """

        spark_conf = self.session.sparkContext.getConf()
        if isinstance(conf_dict, dict):
            conf_kvs = [(k, v) for k, v in conf_dict.items()]
        else:
            conf_kvs = conf_dict
        spark_conf_updated = spark_conf.setAll(conf_kvs)
        self.session.sparkContext.stop()
        spark = SparkSession.builder.config(conf=spark_conf_updated).getOrCreate()
        return spark


spark_conn = SparkConnector()
