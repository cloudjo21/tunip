import os
import unittest
import tempfile
import toml

from tunip.service_config import get_resource_path, get_service_config


class ServiceConfigTest(unittest.TestCase):

    def setUp(self):
        self.servers_config_payload = """
        [servers]
        \t[servers.dev]
        \tip = "127.0.1.1"
        \tlocal_username = "ed"
        """

        self.service_config_payload = """
        {
            "fs": "local",
            "local.username": "ed",
            "spark.master": "local[*]"
        }
        """

        os.environ["NAUTS_DEV_USER"] = "ed"

    def test_init_service_config_by_servers_path(self):
        with tempfile.TemporaryDirectory() as dir:
            resource_path = get_resource_path(env="dev", home_dir=dir, dev_user=os.environ["NAUTS_DEV_USER"])
            os.makedirs(resource_path)
            service_config_path = resource_path + os.sep + "application.json"
            with open(service_config_path, mode='w+') as service_f:
                service_f.write(self.service_config_payload)
                service_f.flush()

            self.assertEqual(service_config_path, f"{dir}/experiments/{os.environ['NAUTS_DEV_USER']}/resources/application.json")

            servers_path = None
            with tempfile.NamedTemporaryFile(mode='w+') as f:
                f.write(self.servers_config_payload)
                f.flush()
                servers_config = toml.load(f)
                servers_path = f.name
                print(servers_path)
                assert servers_config is not None

                service_config = get_service_config(servers_path=servers_path, home_dir=dir)
                self.assertNotEqual(service_config, None)

                self.assertEqual(service_config.filesystem, "local")
                self.assertEqual(service_config.username, os.environ["NAUTS_DEV_USER"])
                self.assertEqual(service_config.spark_master, "local[*]")
