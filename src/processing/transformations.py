from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, mean, stddev, lit

class Transformations:
    """
    Reusable data transformation functions
    """
    
    @staticmethod
    def normalize_column(df: DataFrame, column: str) -> DataFrame:
        """
        Normalize a numeric column to [0,1] range
        """
        stats = df.select(
            mean(col(column)).alias('mean'),
            stddev(col(column)).alias('std')
        ).collect()[0]
        
        mean_val = stats['mean']
        std_val = stats['std']
        
        if std_val == 0:
            return df  # Avoid division by zero
        
        return df.withColumn(
            f"{column}_normalized",
            (col(column) - mean_val) / std_val
        )
    
    @staticmethod
    def encode_categorical(df: DataFrame, column: str) -> DataFrame:
        """
        Encode categorical column with numerical values
        """
        categories = df.select(column).distinct().rdd.flatMap(lambda x: x).collect()
        category_map = {cat: idx for idx, cat in enumerate(categories)}
        
        expr = when(col(column) == lit(None), None)
        for cat, idx in category_map.items():
            expr = expr.when(col(column) == cat, idx)
        expr = expr.otherwise(None)
        
        return df.withColumn(f"{column}_encoded", expr)
    
    @staticmethod
    def bucketize_column(df: DataFrame, column: str, buckets: int) -> DataFrame:
        """
        Bucketize a numeric column into specified number of buckets
        """
        min_max = df.select(
            col(column).cast('double').alias(column)
        ).agg(
            {'min': column, 'max': column}
        ).collect()[0]
        
        min_val, max_val = min_max['min'], min_max['max']
        bucket_width = (max_val - min_val) / buckets
        
        return df.withColumn(
            f"{column}_bucket",
            ((col(column) - min_val) / bucket_width).cast('integer')
        )