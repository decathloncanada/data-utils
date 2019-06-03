import os

from dotenv import load_dotenv
import boto3

import data_utils.df as du  # du for data_utils

load_dotenv()

# Connect to the s3 bucket and extract the compressed csv at the key
session = boto3.session.Session(region_name='eu-west-1')
s3client = session.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

df = du.import_s3_csv_to_df(
    s3client=s3client,
    bucket=os.getenv("BUCKET"),
    key=f'Dkt_canada/shawn_test/test_000.gz'
)

du.convert_df_to_csv(df, filepath='./test.csv')
