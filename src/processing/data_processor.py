from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import countDistinct

class DataProcessor:
    """
    Handles data processing and transformations
    """
    
    def __init__(self, spark_session, logger, config):
        self.spark = spark_session
        self.logger = logger
        self.config = config
    
    def clean_data(self, df):
        """
        Clean data by removing nulls, duplicates, and invalid records
        """
        try:
            self.logger.info("Starting data cleaning process")
            
            initial_count = df.count()
            
            # Remove nulls from critical columns
            df_cleaned = df.filter(
                col("order_id").isNotNull() & 
                col("customer_id").isNotNull() & 
                col("product_id").isNotNull()
            )
            
            # Remove duplicates
            df_cleaned = df_cleaned.dropDuplicates(["order_id"])
            
            # Remove invalid records (negative quantities or prices)
            df_cleaned = df_cleaned.filter(
                (col("quantity") > 0) & 
                (col("price") > 0)
            )
            
            final_count = df_cleaned.count()
            
            self.logger.info(f"Data cleaning completed. Records: {initial_count} -> {final_count}")
            
            return df_cleaned
            
        except Exception as e:
            self.logger.error(f"Error in data cleaning: {str(e)}")
            raise
    
    def add_derived_columns(self, df):
        """
        Add derived columns for analytics
        """
        try:
            self.logger.info("Adding derived columns")
            
            df_enhanced = df.withColumn("total_amount", col("quantity") * col("price")) \
                           .withColumn("order_year", year(col("order_date"))) \
                           .withColumn("order_month", month(col("order_date"))) \
                           .withColumn("order_day", dayofmonth(col("order_date"))) \
                           .withColumn("customer_age_group", 
                                     when(col("customer_age") < 25, "Young")
                                     .when(col("customer_age") < 35, "Adult")
                                     .otherwise("Senior"))
            
            self.logger.info("Derived columns added successfully")
            return df_enhanced
            
        except Exception as e:
            self.logger.error(f"Error adding derived columns: {str(e)}")
            raise
    
    def calculate_customer_metrics(self, df):
        """
        Calculate customer-level metrics
        """
        try:
            self.logger.info("Calculating customer metrics")
            
            customer_metrics = df.groupBy("customer_id", "customer_age", "customer_gender") \
                               .agg(
                                   count("order_id").alias("total_orders"),
                                   sum("total_amount").alias("total_spent"),
                                   avg("total_amount").alias("avg_order_value"),
                                   max("order_date").alias("last_order_date"),
                                   countDistinct("product_id").alias("unique_products")
                               )
            
            # Add customer value segments
            customer_metrics = customer_metrics.withColumn(
                "customer_segment",
                when(col("total_spent") > 1000, "High Value")
                .when(col("total_spent") > 500, "Medium Value")
                .otherwise("Low Value")
            )
            
            self.logger.info("Customer metrics calculated successfully")
            return customer_metrics
            
        except Exception as e:
            self.logger.error(f"Error calculating customer metrics: {str(e)}")
            raise
    
    def calculate_product_metrics(self, df):
        """
        Calculate product-level metrics
        """
        try:
            self.logger.info("Calculating product metrics")
            
            product_metrics = df.groupBy("product_id", "product_name", "category") \
                              .agg(
                                  count("order_id").alias("total_orders"),
                                  sum("quantity").alias("total_quantity_sold"),
                                  sum("total_amount").alias("total_revenue"),
                                  avg("price").alias("avg_price"),
                                  countDistinct("customer_id").alias("unique_customers")
                              )
            
            # Add product performance ranking
            window_spec = Window.partitionBy("category").orderBy(desc("total_revenue"))
            product_metrics = product_metrics.withColumn(
                "category_rank",
                row_number().over(window_spec)
            )
            
            self.logger.info("Product metrics calculated successfully")
            return product_metrics
            
        except Exception as e:
            self.logger.error(f"Error calculating product metrics: {str(e)}")
            raise
    
    def create_time_series_analysis(self, df):
        """
        Create time series analysis data
        """
        try:
            self.logger.info("Creating time series analysis")
            
            daily_metrics = df.groupBy("order_date") \
                            .agg(
                                count("order_id").alias("daily_orders"),
                                sum("total_amount").alias("daily_revenue"),
                                countDistinct("customer_id").alias("daily_unique_customers"),
                                avg("total_amount").alias("daily_avg_order_value")
                            ) \
                            .orderBy("order_date")
            
            # Add moving averages
            window_7_days = Window.orderBy("order_date").rowsBetween(-6, 0)
            
            daily_metrics = daily_metrics.withColumn(
                "revenue_7day_ma",
                avg("daily_revenue").over(window_7_days)
            ).withColumn(
                "orders_7day_ma",
                avg("daily_orders").over(window_7_days)
            )
            
            self.logger.info("Time series analysis created successfully")
            return daily_metrics
            
        except Exception as e:
            self.logger.error(f"Error creating time series analysis: {str(e)}")
            raise