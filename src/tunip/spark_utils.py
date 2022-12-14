from pyspark import SparkConf
from pyspark.sql import SparkSession

from tunip.service_config import get_service_config
from tunip.singleton import Singleton


class SparkConnector(Singleton):
    @property
    def session(self):
        # >= spark 3.0
        active_session = SparkSession.getActiveSession()
        if active_session:
            return active_session
        else:
            return self.getOrCreate()

    def getOrCreate(self, local=False, servers_path=None, force_service_level=None):
        service_config = get_service_config(servers_path, force_service_level)

        spark_conf = SparkConf()
        default_spark_config = {
            "spark.driver.maxResultSize": "8g",
            "spark.sql.broadcastTimeout": "720000",
            "spark.rpc.lookupTimeout": "600s",
            "spark.network.timeout": "600s",
        }

        if service_config.spark_master:
            default_spark_config["spark.master"] = service_config.spark_master
        else:
            if not local:
                default_spark_config["spark.master"] = "yarn"
                default_spark_config["spark.submit.deployMode"] = "client"
            else:
                default_spark_config["spark.master"] = "local"
        default_spark_config["spark.driver.bindAddress"] = "127.0.0.1"

        spark_conf_kvs = [(k, v) for k, v in default_spark_config.items()]

        spark_conf.setAll(spark_conf_kvs)
        spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

        spark.sparkContext._jsc.hadoopConfiguration().set(
            "fs.defaultFS", f"{service_config.filesystem_scheme}/user/{service_config.username}"
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
