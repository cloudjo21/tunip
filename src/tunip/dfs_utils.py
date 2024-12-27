from tunip.service_config import ServiceLevelConfig
from tunip.file_utils import HttpBasedWebHdfsFileHandler
from tunip.file_utils import services as file_services


class DfsDownloader:
    def __init__(self, service_config: ServiceLevelConfig):
        self.service_config = service_config
        self.file_handler = self._get_file_service(service_config)

    def download(self, path) -> bool:
        if self.file_handler and not self.service_config.has_local_fs:
            if self.service_config.has_hdfs_fs:
                self.file_handler.download(path, read_mode='rb', write_mode='wb')
            else:
                self.file_handler.download(path)
            return True
        else:
            return False

    def _get_file_service(self, service_config):
        if service_config.has_local_fs:
            return None
        if service_config.has_hdfs_fs:
            return HttpBasedWebHdfsFileHandler(service_config)
        else:
            return file_services.get(
                self.service_config.filesystem.upper(), config=self.service_config.config
            )
