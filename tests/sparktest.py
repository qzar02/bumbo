import unittest
import logging
from pyspark.sql import SparkSession
import bumbo

class PySparkTest(unittest.TestCase):
    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder
            .master('local[1]')
            .appName('bumbo-pyspark-context')
            .enableHiveSupport()
            .getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()
        bumbo.spark = cls.spark

    # @classmethod
    # def tearDownClass(cls):
    #     cls.spark.stop()