from tunip.env import NAUTS_HOME
from tunip.service_config import ServiceLevelConfig
from tunip.spark import SparkConfigLoader


SPARK_JARS = ",".join([
    f"file:///{NAUTS_HOME}/resources/hadoop-aws-3.3.3.jar",
    f"file:///{NAUTS_HOME}/resources/aws-java-sdk-bundle-1.11.1026.jar"
])


class AWSConfigLoader(SparkConfigLoader):

    def __init__(self, service_config: ServiceLevelConfig):
        super(AWSConfigLoader, self).__init__(service_config)

    def hadoop_config(self) -> dict:
        default_hadoop_config = dict()
        if self.service_config.has_s3_fs:
            # YOU CAN ASSIGN CONFIGS
            pass
        return default_hadoop_config

    def spark_config(self) -> dict:
        default_spark_config = {
            "spark.driver.maxResultSize": "2g",
            "spark.sql.broadcastTimeout": "720000",
            "spark.rpc.lookupTimeout": "600s",
            "spark.network.timeout": "600s",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider":"com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.jars": SPARK_JARS,
        }
        return default_spark_config
