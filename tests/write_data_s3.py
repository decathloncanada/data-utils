import os

import pandas as pd
import boto3
from dotenv import load_dotenv

from data_utils.df import convert_df_to_s3_compressed_csv

load_dotenv()

# Connect to the s3 bucket and extract the compressed csv at the key
session = boto3.session.Session(region_name='eu-west-1')
s3client = session.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

data = {'col1': [1, 2], 'col2': [3, 4]}
df = pd.DataFrame(data=data)

convert_df_to_s3_compressed_csv(
    df,
    s3client=s3client,
    bucket=os.getenv("BUCKET"),
    key=f'Dkt_canada/shawn_test/test_000.gz'
)
