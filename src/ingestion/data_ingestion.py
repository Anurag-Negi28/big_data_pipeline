from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import requests
import json
import os
from datetime import datetime

class DataIngestion:
    """
    Handles data ingestion from various sources
    """
    
    def __init__(self, spark_session, logger, config):
        self.spark = spark_session
        self.logger = logger
        self.config = config
        self.raw_data_path = config['data']['raw_data_path']
    
    def ingest_csv_data(self, file_path, schema=None):
        """
        Ingest CSV data with optional schema
        """
        try:
            self.logger.info(f"Ingesting CSV data from: {file_path}")
            
            if schema:
                df = self.spark.read.schema(schema).csv(file_path, header=True)
            else:
                df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            
            self.logger.info(f"Successfully ingested {df.count()} records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error ingesting CSV data: {str(e)}")
            raise
    
    def ingest_json_data(self, file_path):
        """
        Ingest JSON data
        """
        try:
            self.logger.info(f"Ingesting JSON data from: {file_path}")
            
            df = self.spark.read.json(file_path)
            self.logger.info(f"Successfully ingested {df.count()} records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error ingesting JSON data: {str(e)}")
            raise
    
    def ingest_api_data(self, api_url, headers=None, params=None):
        """
        Ingest data from REST API
        """
        try:
            self.logger.info(f"Ingesting data from API: {api_url}")
            
            response = requests.get(api_url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Convert to Spark DataFrame
            if isinstance(data, list):
                df = self.spark.createDataFrame(data)
            else:
                df = self.spark.createDataFrame([data])
            
            self.logger.info(f"Successfully ingested {df.count()} records from API")
            return df
            
        except Exception as e:
            self.logger.error(f"Error ingesting API data: {str(e)}")
            raise
    
    def create_sample_data(self):
        """
        Create sample e-commerce data for demonstration
        """
        try:
            self.logger.info("Creating sample e-commerce data")
            
            # Sample data schema
            schema = StructType([
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("price", DoubleType(), True),
                StructField("order_date", DateType(), True),
                StructField("customer_age", IntegerType(), True),
                StructField("customer_gender", StringType(), True)
            ])
            
            # Sample data with order_date converted to datetime.date
            sample_data = [
                ("ORD001", "CUST001", "PROD001", "Laptop", "Electronics", 1, 999.99, datetime.strptime("2024-01-15", "%Y-%m-%d").date(), 25, "M"),
                ("ORD002", "CUST002", "PROD002", "Phone", "Electronics", 2, 699.99, datetime.strptime("2024-01-16", "%Y-%m-%d").date(), 30, "F"),
                ("ORD003", "CUST003", "PROD003", "Book", "Education", 3, 29.99, datetime.strptime("2024-01-17", "%Y-%m-%d").date(), 22, "M"),
                ("ORD004", "CUST004", "PROD004", "Headphones", "Electronics", 1, 199.99, datetime.strptime("2024-01-18", "%Y-%m-%d").date(), 28, "F"),
                ("ORD005", "CUST005", "PROD005", "Tablet", "Electronics", 1, 399.99, datetime.strptime("2024-01-19", "%Y-%m-%d").date(), 35, "M"),
                ("ORD006", "CUST001", "PROD006", "Mouse", "Electronics", 2, 49.99, datetime.strptime("2024-01-20", "%Y-%m-%d").date(), 25, "M"),
                ("ORD007", "CUST002", "PROD007", "Keyboard", "Electronics", 1, 79.99, datetime.strptime("2024-01-21", "%Y-%m-%d").date(), 30, "F"),
                ("ORD008", "CUST006", "PROD008", "Monitor", "Electronics", 1, 299.99, datetime.strptime("2024-01-22", "%Y-%m-%d").date(), 40, "M"),
                ("ORD009", "CUST007", "PROD009", "Speaker", "Electronics", 1, 149.99, datetime.strptime("2024-01-23", "%Y-%m-%d").date(), 26, "F"),
                ("ORD010", "CUST008", "PROD010", "Camera", "Electronics", 1, 799.99, datetime.strptime("2024-01-24", "%Y-%m-%d").date(), 32, "M")
            ]
            
            df = self.spark.createDataFrame(sample_data, schema)
            self.logger.info(f"Created DataFrame with {df.count()} records")
            
            # Save sample data
            output_path = os.path.join(self.raw_data_path, "sample_orders.csv")
            try:
                self.logger.info(f"Attempting to save DataFrame to {output_path}")
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                spark_output_path = output_path.replace("\\", "/")
                df.coalesce(1).write.mode("overwrite").option("header", "true").csv(spark_output_path)
                self.logger.info(f"Successfully saved DataFrame to {spark_output_path}")
            except Exception as e:
                self.logger.error(f"Failed to save sample data to {output_path}: {str(e)}")
                raise
            
            self.logger.info(f"Sample data created with {df.count()} records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error creating sample data: {str(e)}")
            raise