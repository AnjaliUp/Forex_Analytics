CREATE EXTERNAL TABLE IF NOT EXISTS forex_analytics.forexdata (
  customer_name STRING,
  sale_date STRING,
  city STRING,
  country STRING,
  local_currency_code STRING,
  local_currency_amount DOUBLE,
  euro_conversion_rate DOUBLE,
  usd_conversion_rate DOUBLE,
  converted_amount_usd DOUBLE,
  converted_amount_eur DOUBLE
)
STORED AS PARQUET
LOCATION 's3://forex-processed/parquet-file/'
TBLPROPERTIES ('classification' = 'parquet');