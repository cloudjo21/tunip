import unittest

from tunip.service_config import get_service_config
from tunip.google_cloud_utils import StorageDriver


class GoogleCloudUtilsTest(unittest.TestCase):

    def setUp(self):
        self.service_config = get_service_config()

    def test_list_buckets(self):
        driver = StorageDriver(self.service_config.config)

        res = driver.client.list_buckets()
        assert res is not None
        for r in res:
            print(r.name)

        bucket = driver.client.get_bucket(self.service_config.config["gcs.bucketname"])
        res = bucket.list_blobs(prefix=f'user/{self.service_config.username}/')
        assert res is not None
        for r in res:
            print(r.name)

        # bucket = driver.client.get_bucket(self.service_config.config["gcs.bucketname"])
        res = driver.client.list_blobs(bucket_or_name=self.service_config.config["gcs.bucketname"], prefix=f'user/{self.service_config.username}/')
        assert res is not None

