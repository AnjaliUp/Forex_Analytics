SELECT * FROM FOREX_ANALYTICS.FOREXDATA;

#Average Exchange Rates 
SELECT AVG(euro_conversion_rate) AS avg_eur_rate,AVG(usd_conversion_rate) AS avg_usd_rate FROM FOREX_ANALYTICS.FOREXDATA;

#Total Sales 
SELECT SUM(local_currency_amount) AS total_local_amount, SUM(converted_amount_usd) AS total_sales_usd, SUM(converted_amount_eur) AS total_sales_eur FROM FOREX_ANALYTICS.FOREXDATA;

#Conversion Trends Over time
SELECT sale_date, SUM(local_currency_amount) AS total_local_amount, SUM(converted_amount_usd) AS total_usd, SUM(converted_amount_eur) AS total_eur, AVG(euro_conversion_rate) AS avg_eur_rate, AVG(usd_conversion_rate) AS avg_usd_rate 
FROM FOREX_ANALYTICS.FOREXDATA 
GROUP BY sale_date 
ORDER BY sale_date;

#Top 5 Traded Currency Pairs By Transaction values-
SELECT local_currency_code AS currency, SUM(converted_amount_usd) AS total_value_usd,
SUM(converted_amount_eur) AS total_value_eur
FROM FOREX_ANALYTICS.FOREXDATA 
GROUP BY local_currency_code
ORDER BY total_value_usd DESC 
LIMIT 5;