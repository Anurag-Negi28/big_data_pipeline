from pyspark.sql import SparkSession
import logging

class SparkSessionManager:
    """
    Manages Spark session creation and configuration
    """
    
    def __init__(self, config_path):
        self.config_path = config_path
        self.logger = logging.getLogger(__name__)
    
    def create_spark_session(self):
        """Create and configure Spark session"""
        try:
            self.logger.info("Creating Spark session")
            spark = (SparkSession.builder
                     .appName("BigDataPipeline")
                     .master("local[2]")
                     .config("spark.executor.memory", "2g")
                     .config("spark.driver.memory", "2g")
                     .config("spark.python.worker.reuse", "true")
                     .config("spark.python.worker.connectionTimeout", "60000")
                     .config("spark.sql.shuffle.partitions", "2")
                     .getOrCreate())
            self.logger.info("Spark session created successfully")
            return spark
        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {str(e)}")
            raise
    
    def stop_spark_session(self):
        """Stop Spark session"""
        try:
            self.logger.info("Stopping Spark session")
            SparkSession.getActiveSession().stop()
            self.logger.info("Spark session stopped successfully")
        except Exception as e:
            self.logger.error(f"Error stopping Spark session: {str(e)}")
            raise