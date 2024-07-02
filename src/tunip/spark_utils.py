from pathlib import Path
from pyspark import SparkConf
from pyspark.sql import SparkSession

from tunip.env import NAUTS_HOME
from tunip.service_config import get_service_config
from tunip.singleton import Singleton


class SparkConnector(Singleton):
    PROPERTY_FOR_SPARK_JARS = ",".join([
        f"file:///{NAUTS_HOME}/resources/gcs-connector-hadoop2-latest.jar",
        f"file:///{NAUTS_HOME}/resources/elasticsearch-spark-20_2.12-8.6.0.jar",
        f"file:///{NAUTS_HOME}/resources/spark-3.3-bigquery-0.32.2.jar",
        f"file:///{NAUTS_HOME}/resources/mysql-connector-j-8.2.0.jar"
    ])

    @property
    def session(self):
        # >= spark 3.0
        active_session = SparkSession.getActiveSession()
        if active_session:
            return active_session
        else:
            return self.get()
    
    def get(self, local=False, servers_path=None, force_service_level=None):
       return SparkConnector.getOrCreate(local, servers_path, force_service_level) 

    @classmethod
    def getOrCreate(cls, local=False, servers_path=None, force_service_level=None, spark_config=None):
        service_config = get_service_config(servers_path, force_service_level)

        user_hadoop_config = {}
        if service_config.has_gcs_fs:
            gcs_keypath = str(Path(NAUTS_HOME) / 'resources' / f"{service_config.config.get('gcs.project_id')}.json")
            gc_user_hadoop_config = {
                'fs.gs.auth.service.account.enable': 'true',
                'google.cloud.auth.service.account.json.keyfile': gcs_keypath,
            }
            user_hadoop_config.update(gc_user_hadoop_config)

        spark_conf = SparkConf()
        default_spark_config = {
            "spark.driver.maxResultSize": "8g",
            "spark.sql.broadcastTimeout": "720000",
            "spark.rpc.lookupTimeout": "600s",
            "spark.network.timeout": "600s",
            'spark.hadoop.fs.gs.impl': 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem',
            'spark.jars': SparkConnector.PROPERTY_FOR_SPARK_JARS,
            "viewsEnabled": "true",
            "materializationDataset": "mysql_to_bigquery",
            "materializationProject": service_config.gcs_project_id,
            # 'spark.jars': f'file:///{NAUTS_HOME}/resources/gcs-connector-hadoop2-latest.jar,file:///{NAUTS_HOME}/resources/elasticsearch-spark-20_2.12-8.6.0.jar'
        }
        if spark_config and isinstance(spark_config, dict):
            default_spark_config.update(spark_config)

        if service_config.spark_master:
            default_spark_config["spark.master"] = service_config.spark_master
        else:
            if not local:
                default_spark_config["spark.master"] = "yarn"
                default_spark_config["spark.submit.deployMode"] = "client"
                default_spark_config["spark.driver.bindAddress"] = "127.0.0.1"
            else:
                default_spark_config["spark.master"] = "local[2]"

        spark_conf_kvs = [(k, v) for k, v in default_spark_config.items()]

        spark_conf.setAll(spark_conf_kvs)
        spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

        spark.sparkContext._jsc.hadoopConfiguration().set(
            "fs.defaultFS", f"{service_config.filesystem_scheme}/user/{service_config.username}"
        )
        if service_config.has_gcs_fs:
            for k, v in user_hadoop_config.items():
                spark.sparkContext._jsc.hadoopConfiguration().set(k, v)

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
