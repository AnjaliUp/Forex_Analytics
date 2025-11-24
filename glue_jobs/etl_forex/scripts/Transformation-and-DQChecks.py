import sys
from datetime import datetime
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

from schemas.customer_schema import customer_schema
from schemas.rate_schema import rate_schema
from dq.dq_checks import apply_dq_checks
from dq.transformations import join_customers_rates, split_valid_invalid, select_final_columns
from awsglue.utils import getResolvedOptions

# --- INIT ---
spark = SparkSession.builder.appName("ForexDQJob").getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# Define the parameters you expect
args = getResolvedOptions(sys.argv, [
    'customer_path',
    'rate_path',
    'output_path',
    'curated_path',
    'error_path'
])

customer_path = args['customer_path']
rate_path     = args['rate_path']
output_path   = args['output_path']
curated_path  = args['curated_path']
error_path    = args['error_path']

# --- LOAD DATA ---
customers_df = spark.read.option("header", True).schema(customer_schema).csv(customer_path)
rates_df     = spark.read.option("header", True).schema(rate_schema).csv(rate_path)

# --- TRANSFORM ---
joined_df = join_customers_rates(customers_df, rates_df)
dq_df = apply_dq_checks(joined_df)
valid_df, invalid_df = split_valid_invalid(dq_df)

# --- WRITE INVALID ---
today_str = datetime.today().strftime("%Y-%m-%d")
error_path = f"{error_path}{today_str}/"
invalid_df.write.mode("overwrite").option("header", True).csv(error_path)

# --- WRITE CLEANED ---
clean_df = select_final_columns(valid_df)
clean_df.write.mode("overwrite").parquet(output_path)
clean_df.write.mode("overwrite").option("header", True).csv(curated_path)

# --- DQ SCORE ---
total_records = dq_df.count()
valid_records = valid_df.count()
dq_score = (valid_records / total_records) * 100 if total_records > 0 else 0
print(f"Data Quality Score: {dq_score:.2f}%")

spark.stop()