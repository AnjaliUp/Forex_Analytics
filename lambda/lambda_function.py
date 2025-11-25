import boto3
import csv
import io

# Initialize the S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Defining source and destination S3 buckets and file names
    source_bucket = 'forex-raw'
    source_file_key = 'currency_conversion_rates_eur_usd.csv'
    destination_bucket = 'forex-processed'
    destination_file_key = 'currency_conversion_rates_eur_usd_processed.csv'
    
    # Fetching the CSV file from the source S3 bucket
    try:
        response = s3_client.get_object(Bucket=source_bucket, Key=source_file_key)
        csv_content = response['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error fetching the file from S3: {str(e)}")
        raise

    # Reading the CSV content
    csv_reader = csv.reader(io.StringIO(csv_content))
    
    # Preparing the processed data to write to destination
    processed_data = []

    # Processing rows from the CSV
    for row in csv_reader:
        processed_data.append(row)

    # Converting into CSV format
    output_csv = io.StringIO()
    csv_writer = csv.writer(output_csv)
    csv_writer.writerows(processed_data)
    
    # Writing to the destination S3 bucket
    try:
        s3_client.put_object(
            Bucket=destination_bucket,
            Key=destination_file_key,
            Body=output_csv.getvalue(),
            ContentType='text/csv'
        )
        print(f"Processed file successfully uploaded to s3://{destination_bucket}/{destination_file_key}")
    except Exception as e:
        print(f"Error uploading the processed file to S3: {str(e)}")
        raise

    return {
        'statusCode': 200,
        'body': f"File processed and saved to {destination_bucket}/{destination_file_key}"
    }
