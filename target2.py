import pandas as pd
import boto3
from io import BytesIO
from botocore.exceptions import NoCredentialsError

# Your AWS credentials and S3 bucket information
aws_access_key_id = 'oC/G6SbIYU9RkmpVLWh0S8gjZ/97KlYKfvuKVovj'
aws_secret_access_key = 'AKIAXDEGH27Q2JBFQVKU'
region_name = 'us-east-1'  # Replace with your AWS region
bucket_name = 'sk-spotify-etl'
file_key = 'top_artist_today.csv'

# Download the existing file from S3
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  region_name=region_name)

try:
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    existing_data = pd.read_csv(response['Body'])
except NoCredentialsError:
    print("Credentials not available")

# Append new data to the DataFrame
new_data = {'Name': ['David', 'Eva'],
            'Age': [40, 22],
            'City': ['Los Angeles', 'Chicago']}
new_df = pd.DataFrame(new_data)

appended_data = existing_data.append(new_df, ignore_index=True)

# Upload the modified file back to S3
upload_buffer = BytesIO()
appended_data.to_csv(upload_buffer, index=False)
upload_buffer.seek(0)

s3.put_object(Body=upload_buffer.getvalue(), Bucket=bucket_name, Key=file_key)

print(f'Data appended and uploaded to s3://{bucket_name}/{file_key}')
