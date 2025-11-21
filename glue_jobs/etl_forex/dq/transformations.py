from pyspark.sql.functions import col

def join_customers_rates(customers_df, rates_df):
    rates_df = rates_df.dropDuplicates(["currency_code"])
    return customers_df.join(
        rates_df,
        customers_df.local_currency_code == rates_df.currency_code,
        "left"
    )

def split_valid_invalid(dq_df):
  
    valid_records = dq_df.filter(col("is_valid") == True)
    invalid_records = dq_df.filter(col("is_valid") == False)
    return valid_records, invalid_records

def select_final_columns(valid_df):
    
    final_cols = [
        "customer_name", "sale_date", "city", "country",
        "local_currency_code", "local_currency_amount",
        "euro_conversion_rate", "usd_conversion_rate",
        "converted_amount_usd", "converted_amount_eur"
    ]
    return valid_df.drop("rate_country").select(*final_cols)


