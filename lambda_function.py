import boto3
import pandas as pd
import pg8000
from io import StringIO
import json

# Initialize Boto3 clients for S3 and Redshift
s3_client = boto3.client('s3')
redshift_client = boto3.client('redshift')

def lambda_handler(event, context):
    print("event",event)
    # Retrieve input parameters from Lambda event
    input_bucket = event['input_bucket']
    input_key = event['input_key']
    output_bucket = event['output_bucket']
    redshift_params = event['redshift_params']

    # Read subset data from S3
    response = s3_client.get_object(Bucket=input_bucket, Key=input_key)
    df = pd.read_csv(response['Body'])
    

    # Perform feature engineering (example: add new columns)
    df= generate_features(df)
    symbol= df["Symbol"][0]
    # Save processed data to S3
    processed_output_key = f"processed/subset_{symbol}.csv"
    processed_s3_path = f"s3://{processed_output_key}"
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=output_bucket, Key=processed_output_key, Body=csv_buffer.getvalue())
    # Insert records into Redshift
    insert_records_to_redshift(df, redshift_params)

def insert_records_to_redshift(df, redshift_params):
    # Connect to Redshift database
    conn = pg8000.connect(
        user=redshift_params['user'],
        password=redshift_params['password'],
        host=redshift_params['host'],
        port=redshift_params['port'],
        database=redshift_params['dbname']  # Use 'database' instead of 'dbname'
    )
    print("con",)
    cur = conn.cursor()

    # Insert records into Redshift table
    for index, row in df.iterrows():
        # Example: Insert into "tablename" (column1, column2, ...) values (value1, value2, ...)
        insert_query = f"INSERT INTO {redshift_params['tablename']} ({', '.join(df.columns)}) VALUES ({', '.join(map(str, row.values))})"
        cur.execute(insert_query)

    # Commit and close connection
    conn.commit()
    conn.close()

    print("Records inserted into Redshift successfully")

def generate_features(df):
    # Time-based Features
    df['Date'] = pd.to_datetime(df['Date'])
    df['DayOfWeek'] = df['Date'].dt.dayofweek
    df['Month'] = df['Date'].dt.month
    df['Quarter'] = df['Date'].dt.quarter
    df['Year'] = df['Date'].dt.year
    
    # Rolling Statistics
    window_sizes = [7, 14, 30]  # Define different rolling window sizes
    for window in window_sizes:
        df[f'MA_{window}'] = df['Close'].rolling(window).mean()
        df[f'STD_{window}'] = df['Close'].rolling(window).std()
    
    # Technical Indicators
    # Add code to calculate RSI, MACD, Bollinger Bands
    
    # Lag Features
    lag_periods = [1, 2, 3]  # Define different lag periods
    for lag in lag_periods:
        df[f'Lag_{lag}'] = df['Close'].shift(lag)
    
    # Volume-based Features
    df['Volume_MA'] = df['Volume'].rolling(window=20).mean()
    df['Volume_Ratio'] = df['Volume'] / df['Volume'].rolling(window=20).mean()
    
    # High-Low Range Features
    df['High_Low_Percentage'] = (df['High'] - df['Low']) / df['Low'] * 100
    
    return df