from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

class DataStorage:
    """
    Handles data storage operations
    """
    
    def __init__(self, spark_session, logger, config):
        self.spark = spark_session
        self.logger = logger
        self.config = config
        self.processed_data_path = config['data']['processed_data_path']
        self.output_data_path = config['data']['output_data_path']
    
    def save_to_parquet(self, df, path, partition_cols=None, mode="overwrite"):
        """
        Save DataFrame to Parquet format with optional partitioning
        """
        try:
            self.logger.info(f"Saving data to Parquet: {path}")
            
            writer = df.write.mode(mode)
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.parquet(path)
            
            self.logger.info(f"Data successfully saved to {path}")
            
        except Exception as e:
            self.logger.error(f"Error saving to Parquet: {str(e)}")
            raise
    
    def save_to_delta(self, df, path, partition_cols=None, mode="overwrite"):
        """
        Save DataFrame to Delta format (if Delta Lake is available)
        """
        try:
            self.logger.info(f"Saving data to Delta: {path}")
            
            writer = df.write.format("delta").mode(mode)
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.save(path)
            
            self.logger.info(f"Data successfully saved to Delta format at {path}")
            
        except Exception as e:
            self.logger.error(f"Error saving to Delta: {str(e)}")
            raise
    
    def save_to_csv(self, df, path, mode="overwrite", header=True):
        """
        Save DataFrame to CSV format
        """
        try:
            self.logger.info(f"Saving data to CSV: {path}")
            
            df.coalesce(1).write.mode(mode).option("header", header).csv(path)
            
            self.logger.info(f"Data successfully saved to CSV at {path}")
            
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {str(e)}")
            raise
    
    def create_data_lake_structure(self, df, base_path):
        """
        Create a data lake structure with bronze, silver, gold layers
        """
        try:
            self.logger.info("Creating data lake structure")
            
            # Bronze layer (raw data)
            bronze_path = os.path.join(base_path, "bronze")
            self.save_to_parquet(df, bronze_path)
            
            # Silver layer (cleaned data)
            silver_df = df.dropna().dropDuplicates()
            silver_path = os.path.join(base_path, "silver")
            self.save_to_parquet(silver_df, silver_path, partition_cols=["category"])
            
            # Gold layer (aggregated data)
            gold_df = silver_df.groupBy("category").agg(
                count("*").alias("total_orders"),
                sum("quantity").alias("total_quantity"),
                avg("price").alias("avg_price"),
                max("order_date").alias("latest_order_date")
            )
            gold_path = os.path.join(base_path, "gold")
            self.save_to_parquet(gold_df, gold_path)
            
            self.logger.info("Data lake structure created successfully")
            
        except Exception as e:
            self.logger.error(f"Error creating data lake structure: {str(e)}")
            raise