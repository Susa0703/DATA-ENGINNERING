import sys
import boto3
import pandas as pd
import json

# Get the job arguments
args = {
    'input_bucket': 'final-project-stock-predict-1',
    'input_key': 'linear_data.csv',
    'output_bucket': 'final-project-stock-predict-1',
    'queue_name': 'your_queue_name'  # Replace with your SQS queue name
}

# Initialize Boto3 client for S3
s3 = boto3.client('s3')

# Initialize Boto3 client for SQS
sqs = boto3.client('sqs')

def split_csv_data(input_bucket, input_key, output_bucket, queue_name):
    # Read CSV file from S3
    response = s3.get_object(Bucket=input_bucket, Key=input_key)
    df = pd.read_csv(response['Body'])

    # Split data based on 'symbol' column
    symbols = df['Symbol'].unique()
    for symbol in symbols:
        subset = df[df['Symbol'] == symbol]
        subset_key = f"{output_bucket}/subset_{symbol}.csv"
        subset.to_csv(f"s3://{subset_key}", index=False)

        # Send message to SQS queue
        sqs_message = {
            'input_bucket': output_bucket,
            'input_key': f"subset_{symbol}.csv",
            'output_bucket': input_bucket,
            'redshift_params': {
                'user': 'awsuser',
                'password': 'Admin123',
                'host': '',
                'port': 5439,
                'dbname': 'database_linear'
            }
        }
        sqs.send_message(
            QueueUrl=queue_name,
            MessageBody=json.dumps(sqs_message)
        )
        print("Message sent to SQS queue")

# Get input parameters
input_bucket = args['input_bucket']
input_key = args['input_key']
output_bucket = args['output_bucket']
queue_name = args['queue_name']

# Split CSV data and send message to SQS for each subset
split_csv_data(input_bucket, input_key, output_bucket, queue_name)
