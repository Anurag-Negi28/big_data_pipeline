from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import os

class DataServing:
    """
    Handles data serving and output operations
    """
    
    def __init__(self, spark_session, logger, config):
        self.spark = spark_session
        self.logger = logger
        self.config = config
        self.output_path = config['data']['output_data_path']
    
    def generate_business_report(self, df, customer_metrics, product_metrics, time_series):
        """
        Generate comprehensive business report
        """
        try:
            self.logger.info("Generating business report")
            
            # Overall business metrics
            total_orders = df.count()
            total_revenue = df.agg(sum("total_amount")).collect()[0][0]
            unique_customers = df.select("customer_id").distinct().count()
            unique_products = df.select("product_id").distinct().count()
            
            # Top performing products
            top_products = product_metrics.orderBy(desc("total_revenue")).limit(5)
            
            # Top customers
            top_customers = customer_metrics.orderBy(desc("total_spent")).limit(5)
            
            # Category performance
            category_performance = df.groupBy("category") \
                                   .agg(
                                       count("order_id").alias("orders"),
                                       sum("total_amount").alias("revenue")
                                   ) \
                                   .orderBy(desc("revenue"))
            
            # Save reports
            report_path = os.path.join(self.output_path, "business_reports")
            os.makedirs(report_path, exist_ok=True)
            
            # Save individual reports
            top_products.coalesce(1).write.mode("overwrite").option("header", "true") \
                       .csv(os.path.join(report_path, "top_products"))
            
            top_customers.coalesce(1).write.mode("overwrite").option("header", "true") \
                        .csv(os.path.join(report_path, "top_customers"))
            
            category_performance.coalesce(1).write.mode("overwrite").option("header", "true") \
                               .csv(os.path.join(report_path, "category_performance"))
            
            # Create summary report
            summary_report = {
                "business_summary": {
                    "total_orders": total_orders,
                    "total_revenue": float(total_revenue) if total_revenue else 0,
                    "unique_customers": unique_customers,
                    "unique_products": unique_products,
                    "avg_order_value": float(total_revenue / total_orders) if total_orders > 0 else 0
                }
            }
            
            # Save summary as JSON
            with open(os.path.join(report_path, "business_summary.json"), "w") as f:
                json.dump(summary_report, f, indent=2)
            
            self.logger.info("Business report generated successfully")
            return summary_report
            
        except Exception as e:
            self.logger.error(f"Error generating business report: {str(e)}")
            raise
    
    def create_data_api_format(self, df):
        """
        Create data in API-friendly format
        """
        try:
            self.logger.info("Creating API-friendly data format")
            
            # Convert to JSON format
            api_data = df.select("*").collect()
            
            # Convert to list of dictionaries
            api_format = []
            for row in api_data:
                row_dict = row.asDict()
                # Convert date objects to strings
                for key, value in row_dict.items():
                    if hasattr(value, 'strftime'):
                        row_dict[key] = value.strftime('%Y-%m-%d')
                api_format.append(row_dict)
            
            # Save as JSON
            api_output_path = os.path.join(self.output_path, "api_data.json")
            with open(api_output_path, "w") as f:
                json.dump(api_format, f, indent=2)
            
            self.logger.info(f"API data format created: {api_output_path}")
            return api_format
            
        except Exception as e:
            self.logger.error(f"Error creating API format: {str(e)}")
            raise
    
    def export_to_formats(self, df, filename_prefix):
        """
        Export data to multiple formats
        """
        try:
            self.logger.info(f"Exporting data in multiple formats: {filename_prefix}")
            
            export_path = os.path.join(self.output_path, "exports")
            os.makedirs(export_path, exist_ok=True)
            
            # Export to CSV
            df.coalesce(1).write.mode("overwrite").option("header", "true") \
              .csv(os.path.join(export_path, f"{filename_prefix}_csv"))
            
            # Export to Parquet
            df.write.mode("overwrite").parquet(os.path.join(export_path, f"{filename_prefix}_parquet"))
            
            # Export to JSON
            df.coalesce(1).write.mode("overwrite").json(os.path.join(export_path, f"{filename_prefix}_json"))
            
            self.logger.info("Data exported to multiple formats successfully")
            
        except Exception as e:
            self.logger.error(f"Error exporting data: {str(e)}")
            raise
