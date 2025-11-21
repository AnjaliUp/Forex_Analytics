from functools import reduce
from pyspark.sql.functions import col, lit, when, round

def apply_dq_checks(joined_df):
    # Round conversion rates to 6 decimal places
    joined_df = joined_df.withColumn("usd_conversion_rate", round(col("usd_conversion_rate"), 6)) \
                         .withColumn("euro_conversion_rate", round(col("euro_conversion_rate"), 6))

    # Converted amounts
    joined_df = joined_df.withColumn(
        "converted_amount_usd",
        col("local_currency_amount") * col("usd_conversion_rate")
    ).withColumn(
        "converted_amount_eur",
        col("local_currency_amount") * col("euro_conversion_rate")
    )

    # Mandatory fields must be non-null
    mandatory_fields = ["sale_date", "local_currency_code", "usd_conversion_rate", "local_currency_amount"]
    non_null_checks = [col(f).isNotNull() for f in mandatory_fields]
    all_non_null = reduce(lambda a, b: a & b, non_null_checks)

    joined_df = joined_df.withColumn(
        "mandatory_fields_present",
        when(all_non_null, lit(True)).otherwise(lit(False))
    )

    # Non-negative check
    joined_df = joined_df.withColumn(
        "non_negative",
        when(
            (col("local_currency_amount") >= 0) &
            (col("converted_amount_usd").isNotNull()) & (col("converted_amount_usd") >= 0) &
            (col("converted_amount_eur").isNotNull()) & (col("converted_amount_eur") >= 0),
            lit(True)
        ).otherwise(lit(False))
    )

    # Final validity flag
    joined_df = joined_df.withColumn(
        "is_valid",
        col("mandatory_fields_present") & col("non_negative")
    )

    return joined_df

