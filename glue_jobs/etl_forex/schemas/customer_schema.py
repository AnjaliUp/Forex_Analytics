from pyspark.sql.types import StructType, StructField, StringType, DoubleType

customer_schema = StructType([
    StructField("customer_name", StringType(), True),
    StructField("sale_date", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("local_currency_code", StringType(), True),
    StructField("local_currency_amount", DoubleType(), True)
])