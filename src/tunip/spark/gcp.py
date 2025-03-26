from pathlib import Path

from tunip.service_config import ServiceLevelConfig
from tunip.spark import SparkConfigLoader


class GCPConfigLoader(SparkConfigLoader):

    def __init__(self, service_config: ServiceLevelConfig):
        super(GCPConfigLoader, self).__init__(service_config)

    def hadoop_config(self) -> dict:
        default_hadoop_config = dict()
        if self.service_config.has_gcs_fs:
            gcs_keypath = str(Path(self.service_config.home_dir) / "resources" / f"{self.service_config.config.get('gcs.project_id')}.json")
            default_hadoop_config = {
                "fs.gs.auth.service.account.enable": "true",
                "google.cloud.auth.service.account.json.keyfile": gcs_keypath,
            }
        return default_hadoop_config

    def spark_config(self) -> dict:
        default_spark_config = {
            "spark.driver.maxResultSize": "8g",
            "spark.sql.broadcastTimeout": "720000",
            "spark.rpc.lookupTimeout": "600s",
            "spark.network.timeout": "600s",
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.jars": self._spark_jars(self.service_config.home_dir),
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "materializationDataset": self.service_config.access_config("materializationDataset"),
            "materializationProject": self.service_config.gcs_project_id
        }
        return default_spark_config

    def _spark_jars(self, home_dir: str):
        return ",".join([
            f"{home_dir}/resources/gcs-connector-hadoop2-latest.jar",
            f"{home_dir}/resources/elasticsearch-spark-20_2.12-8.6.0.jar",
            f"{home_dir}/resources/spark-3.3-bigquery-0.32.2.jar",
            f"{home_dir}/resources/mysql-connector-j-8.2.0.jar"
        ])
