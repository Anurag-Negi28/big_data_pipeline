#!/usr/bin/env python3
"""
Big Data Pipeline Main Orchestrator
Coordinates ingestion, processing, storage, and serving
"""

import os
import sys
import yaml
from datetime import datetime

# Add src to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from utils.spark_session import SparkSessionManager
from utils.logger import PipelineLogger
from ingestion.data_ingestion import DataIngestion
from storage.data_storage import DataStorage
from processing.data_processor import DataProcessor
from serving.data_serving import DataServing

class BigDataPipeline:
    """
    Main Big Data Pipeline Class
    """
    
    def __init__(self, config_path="config/pipeline_config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
        
        # Initialize components
        self.logger = PipelineLogger(
            log_level=self.config['logging']['level'],
            log_file=self.config['logging']['file_path']
        )
        
        self.spark_manager = SparkSessionManager(config_path)
        self.spark = self.spark_manager.create_spark_session()
        
        # Initialize pipeline components
        self.ingestion = DataIngestion(self.spark, self.logger, self.config)
        self.storage = DataStorage(self.spark, self.logger, self.config)
        self.processor = DataProcessor(self.spark, self.logger, self.config)
        self.serving = DataServing(self.spark, self.logger, self.config)
    
    def _load_config(self):
        """Load pipeline configuration"""
        with open(self.config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def run_pipeline(self):
        """
        Execute the complete big data pipeline
        """
        try:
            self.logger.info("=" * 50)
            self.logger.info("STARTING BIG DATA PIPELINE")
            self.logger.info("=" * 50)
            
            # Step 1: Data Ingestion
            self.logger.info("Step 1: Data Ingestion")
            raw_df = self.ingestion.create_sample_data()
            
            # Alternative: Ingest from file
            # raw_df = self.ingestion.ingest_csv_data("data/raw/orders.csv")
            
            # Step 2: Data Processing
            self.logger.info("Step 2: Data Processing")
            
            # Clean data
            cleaned_df = self.processor.clean_data(raw_df)
            
            # Add derived columns
            enhanced_df = self.processor.add_derived_columns(cleaned_df)
            
            # Calculate metrics
            customer_metrics = self.processor.calculate_customer_metrics(enhanced_df)
            product_metrics = self.processor.calculate_product_metrics(enhanced_df)
            time_series = self.processor.create_time_series_analysis(enhanced_df)
            
            # Step 3: Data Storage
            self.logger.info("Step 3: Data Storage")
            
            # Create data lake structure
            self.storage.create_data_lake_structure(enhanced_df, "data/processed/data_lake")
            
            # Save processed datasets
            self.storage.save_to_parquet(
                customer_metrics, 
                "data/processed/customer_metrics",
                partition_cols=["customer_segment"]
            )
            
            self.storage.save_to_parquet(
                product_metrics, 
                "data/processed/product_metrics",
                partition_cols=["category"]
            )
            
            self.storage.save_to_parquet(
                time_series, 
                "data/processed/time_series"
            )
            
            # Step 4: Data Serving
            self.logger.info("Step 4: Data Serving")
            
            # Generate business reports
            business_report = self.serving.generate_business_report(
                enhanced_df, customer_metrics, product_metrics, time_series
            )
            
            # Create API-friendly format
            api_data = self.serving.create_data_api_format(enhanced_df.limit(100))
            
            # Export to multiple formats
            self.serving.export_to_formats(customer_metrics, "customer_metrics")
            self.serving.export_to_formats(product_metrics, "product_metrics")
            
            self.logger.info("=" * 50)
            self.logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 50)
            
            return business_report
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise
        
        finally:
            # Clean up resources
            self.spark_manager.stop_spark_session()
    
    def run_sql_examples(self):
        """
        Demonstrate SQL on distributed systems
        """
        try:
            self.logger.info("Running SQL Examples on Distributed Systems")
            
            # Create sample data
            df = self.ingestion.create_sample_data()
            enhanced_df = self.processor.add_derived_columns(df)
            
            # Register as temporary view for SQL queries
            enhanced_df.createOrReplaceTempView("orders")
            
            # Example SQL queries
            sql_queries = [
                {
                    "name": "Total Revenue by Category",
                    "query": """
                        SELECT category, 
                               COUNT(*) as total_orders,
                               SUM(total_amount) as total_revenue,
                               AVG(total_amount) as avg_order_value
                        FROM orders 
                        GROUP BY category 
                        ORDER BY total_revenue DESC
                    """
                },
                {
                    "name": "Customer Analysis",
                    "query": """
                        SELECT customer_age_group,
                               customer_gender,
                               COUNT(DISTINCT customer_id) as unique_customers,
                               AVG(total_amount) as avg_spending
                        FROM orders
                        GROUP BY customer_age_group, customer_gender
                        ORDER BY avg_spending DESC
                    """
                },
                {
                    "name": "Monthly Trends",
                    "query": """
                        SELECT order_month,
                               COUNT(*) as orders,
                               SUM(total_amount) as total_revenue,
                               COUNT(DISTINCT customer_id) as unique_customers
                        FROM orders
                        GROUP BY order_month
                        ORDER BY order_month
                    """
                }
            ]
            
            # Execute SQL queries
            for query_info in sql_queries:
                self.logger.info(f"Executing: {query_info['name']}")
                result_df = self.spark.sql(query_info['query'])
                result_df.show()
                
                # Save results
                output_path = os.path.join("data", "output", "sql_results", query_info['name'].lower().replace(' ', '_'))
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                spark_output_path = output_path.replace("\\", "/")
                result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(spark_output_path)
            
        except Exception as e:
            self.logger.error(f"Error running SQL examples: {str(e)}")
            raise

def main():
    """Main function to run the pipeline"""
    try:
        # Initialize pipeline
        pipeline = BigDataPipeline()
        
        # Run SQL examples
        pipeline.run_sql_examples()
        
        # Run complete pipeline
        result = pipeline.run_pipeline()
        
        print("\n" + "="*50)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*50)
        print(f"Business Summary: {result}")
        
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()