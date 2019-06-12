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

