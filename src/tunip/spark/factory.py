from tunip.service_config import ServiceLevelConfig
from tunip.spark import SparkConfigLoader
from tunip.spark.aws import AWSConfigLoader
from tunip.spark.gcp import GCPConfigLoader


class DummyConfigLoader(SparkConfigLoader):

    def __init__(self, service_config: ServiceLevelConfig):
        super(DummyConfigLoader, self).__init__(service_config)

    def hadoop_config(self) -> dict:
        return dict()

    def spark_config(self) -> dict:
        return {
            "spark.sql.execution.arrow.enabled": "true"
        }


class SparkConfigLoaderFactory:

    @classmethod
    def create(cls, service_config: ServiceLevelConfig) -> SparkConfigLoader:
        cloud_service_provider = service_config.csp
        if cloud_service_provider == "aws":
            config_loader = AWSConfigLoader(service_config)
        elif cloud_service_provider == "gcp":
            config_loader = GCPConfigLoader(service_config)
        else:
            config_loader = DummyConfigLoader(service_config)
        return config_loader

