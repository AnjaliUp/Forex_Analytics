from pyspark.sql.types import StructType, StructField, StringType, DoubleType

rate_schema = StructType([
    StructField("rate_country", StringType(), True),   # renamed from "country"
    StructField("currency_code", StringType(), True),
    StructField("euro_conversion_rate", DoubleType(), True),
    StructField("usd_conversion_rate", DoubleType(), True)
])
