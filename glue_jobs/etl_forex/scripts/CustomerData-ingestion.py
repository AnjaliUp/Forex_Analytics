import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import col, to_date, upper

# Initializing Glue & Spark
args = getResolvedOptions(sys.argv,
                         ['RAW_SALES_PATH', 'PROCESSED_SALES_PATH','CURATED_SALES_PATH'])
raw_sales_path = args['RAW_SALES_PATH']
processed_sales_path = args['PROCESSED_SALES_PATH']
curated_sales_path= args['CURATED_SALES_PATH']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Defining Schema
sales_schema = StructType([
   StructField("customer_name", StringType(), True),
   StructField("city", StringType(), True),
   StructField("country", StringType(), True),
   StructField("local_currency_amount", DoubleType(), True),
   StructField("local_currency_code", StringType(), True),
   StructField("sale_date",StringType(), True)
])

# Reading Data
sales_df = spark.read.csv(raw_sales_path, schema=sales_schema, header=True)

# Basic Transformations
sales_df = sales_df.withColumn("local_currency_code", upper(col("local_currency_code")))

sales_df = sales_df.withColumn("sale_date", to_date(col("sale_date"), "dd-MM-yyyy")) # date formatting

final_df = sales_df.select("customer_name", "sale_date", "city", "country", "local_currency_code", "local_currency_amount")

# Processed layer
final_df.write.mode("overwrite").parquet(processed_sales_path)

# Curated layer
final_df.write.mode("overwrite").csv(curated_sales_path, header=True)
