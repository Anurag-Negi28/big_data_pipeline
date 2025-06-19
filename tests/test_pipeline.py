import unittest
import os
from pyspark.sql import SparkSession
from src.utils.spark_session import SparkSessionManager
from src.utils.logger import PipelineLogger
from src.ingestion.data_ingestion import DataIngestion
from src.storage.data_storage import DataStorage
from src.processing.data_processor import DataProcessor
from pyspark.sql.functions import col

class TestBigDataPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark_manager = SparkSessionManager("config/pipeline_config.yaml")
        cls.spark = cls.spark_manager.create_spark_session()
        cls.logger = PipelineLogger()
        cls.config = cls._load_config()
    
    @classmethod
    def _load_config(cls):
        import yaml
        with open("config/pipeline_config.yaml", 'r') as file:
            return yaml.safe_load(file)
    
    def test_data_ingestion(self):
        ingestion = DataIngestion(self.spark, self.logger, self.config)
        df = ingestion.create_sample_data()
        self.assertGreater(df.count(), 0, "Sample data should have records")
        self.assertEqual(len(df.columns), 10, "Sample data should have 10 columns")
    
    def test_data_cleaning(self):
        ingestion = DataIngestion(self.spark, self.logger, self.config)
        processor = DataProcessor(self.spark, self.logger, self.config)
        raw_df = ingestion.create_sample_data()
        cleaned_df = processor.clean_data(raw_df)
        self.assertTrue(cleaned_df.filter(col("order_id").isNull()).count() == 0, 
                       "Cleaned data should have no null order IDs")
    
    def test_data_storage(self):
        ingestion = DataIngestion(self.spark, self.logger, self.config)
        storage = DataStorage(self.spark, self.logger, self.config)
        df = ingestion.create_sample_data()
        test_path = "data/test_output"
        storage.save_to_parquet(df, test_path)
        self.assertTrue(os.path.exists(test_path), "Parquet files should be saved")
    
    @classmethod
    def tearDownClass(cls):
        cls.spark_manager.stop_spark_session()

if __name__ == '__main__':
    unittest.main()