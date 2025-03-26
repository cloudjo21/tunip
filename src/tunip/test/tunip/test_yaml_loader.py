import os
import tempfile
import unittest

from tunip.yaml_loader import YamlLoader


class TestYamlLoader(unittest.TestCase):

  def setUp(self):
    self.yaml_payload = """
    dag_flow_test:
      - task_id: extract_records
        ingress:
          domain_name: domain_test
          schema_type: record_test
        egress:
          domain_name: domain_test2
          schema_type: record_test2
    """
  
  def test_load(self):
    with tempfile.TemporaryDirectory() as dir:
      yaml_path = dir + os.sep + "dag.yml"
      with open(yaml_path, mode="w+") as f:
        f.write(self.yaml_payload)
        f.flush()
      yaml_loader = YamlLoader(yaml_path)
      dag_config = yaml_loader.load()
      self.assertEqual(dag_config['dag_flow_test'][0]['task_id'], "extract_records")
      self.assertEqual(dag_config.dag_flow_test[0].task_id, "extract_records")

if __name__ == '__main__':
  unittest.main()
