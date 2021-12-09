import pandas as pd
import unittest

from tunip.spark_utils import spark_conn
from pyspark.sql.types import StructType, StructField, StringType


class SparkUtilsTest(unittest.TestCase):

    def test_update_spark_session_with_config(self):

        spark = spark_conn.session
        before_driver_mem = spark.sparkContext.getConf().get('spark.driver.memory')
        print(f"BEFORE.spark.driver.memory: {before_driver_mem}")

        spark = spark_conn.update({'spark.driver.memory': '16g'})
        after_driver_mem = spark.sparkContext.getConf().get('spark.driver.memory')
        print(f"AFTER.spark.driver.memory: {after_driver_mem}")

        assert before_driver_mem != after_driver_mem
        
    def test_spark_deployMode(self):
        
        spark = spark_conn.session
        
        master = spark.conf.get('spark.master')
        deploy_mode = spark.conf.get('spark.submit.deployMode')
        
        assert master == "yarn" 
        assert deploy_mode == "client"
    
    def test_spark_action(self):
        
        spark = spark_conn.session
        
        hdfs_save_path = "/user/nauts/abc"
        
        df_schema = StructType(
            [
                StructField("language", StringType(), True),
                StructField("users_count", StringType(), True)
            ]
        )
        
        columns = ["language","users_count"]
        lang_list = ["Java", "Python", "Scala"]
        count_list = ["20000", "10000", "30000"]
        
        df = pd.DataFrame(
            zip(lang_list, count_list), 
            columns = columns
        )

        spark_df = spark.createDataFrame(df, df_schema).coalesce(1)
        
        spark_df.write.format('json').mode("overwrite").option(
            "header", "true").save(hdfs_save_path)
        
        df = spark.read.json(hdfs_save_path)
        
        df.show()
        
        assert df.count()==3
        
if __name__=="__main__":
    unittest.main()
