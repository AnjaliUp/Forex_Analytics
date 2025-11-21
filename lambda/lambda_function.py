import boto3
import csv
import io

# Initialize the S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Define source and destination S3 buckets and file names
    source_bucket = 'forex-raw'
    source_file_key = 'currency_conversion_rates_eur_usd.csv'
    destination_bucket = 'forex-processed'
    destination_file_key = 'currency_conversion_rates_eur_usd_processed.csv'
    
    # Fetch the CSV file from the source S3 bucket
    try:
        response = s3_client.get_object(Bucket=source_bucket, Key=source_file_key)
        csv_content = response['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error fetching the file from S3: {str(e)}")
        raise

    # Read the CSV content
    csv_reader = csv.reader(io.StringIO(csv_content))
    
    # Prepare the processed data to write to destination
    processed_data = []

    # Process rows from the CSV
    for row in csv_reader:
        # Here you can modify each row as per your business logic
        # For now, just pass the rows as they are
        processed_data.append(row)

    # Convert the processed data back into CSV format
    output_csv = io.StringIO()
    csv_writer = csv.writer(output_csv)
    csv_writer.writerows(processed_data)
    
    # Write the processed CSV back to the destination S3 bucket
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
