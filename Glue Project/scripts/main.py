# importing the necessary modules

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from pyspark.sql import types as t
import logging as log


class ETLPipeline:

    def __init__(self, source_path: str, target_path: str, spark: SparkSession):
        self.source_path = source_path
        self.target_path = target_path
        self.spark = spark

    def extract(self) -> DataFrame:
        """read data from s3"""

        log.info(f"Reading data from source_path : {self.source_path}")

        df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(self.source_path)
        
        log.info(df.show(5, truncate=False))
        log.info(df.printSchema())

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """Implement business logic to get the required data"""

        log.info("Applying business logic")

        df = df.select(['Region', 'Country', 'Sales'])

        # converting sales : string to float value
        df = df.withColumn('Sales', df['Sales'].cast('float').alias('Sales'))

        log.info(f"Transformed schema : {df.printSchema()}")

        df = df.groupBy(
                ['Region', 'Country']
            ).agg(
                sf.sum('Sales').alias('TotalSales')
            )

        return df
        
    def load(self, df: DataFrame) -> None:
        """write data to target path"""

        df.write.parquet(self.target_path, mode='overwrite')

        log.info(f"Successfully wrote data to {self.target_path}")


if __name__ == "__main__":
    # Creating a SparkContext and GlueContext
    source_path = "s3://clf-glu-demo501/scripts/Superstore.csv"
    target_path = "s3a://cfn-demo501/target"

    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    etl = ETLPipeline(source_path, target_path, spark)
    src_df = etl.extract()
    tf_df = etl.transform(src_df)
    etl.load(tf_df)

    
