import unittest

from tunip.spark_utils import spark_conn


class SparkUtilsTest(unittest.TestCase):

    def test_update_spark_session_with_config(self):

        spark = spark_conn.session
        before_driver_mem = spark.sparkContext.getConf().get('spark.driver.memory')
        print(f"BEFORE.spark.driver.memory: {driver_mem}")

        spark = spark_conn.update_session({'spark.driver.memory': '16g'})
        after_driver_mem = spark.sparkContext.getConf().get('spark.driver.memory')
        print(f"AFTER.spark.driver.memory: {driver_mem}")

        assert before_driver_mem != after_driver_mem
