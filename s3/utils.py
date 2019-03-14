"""
This module contains helper functions to import data from an s3 bucket to a .csv
(For refactoring purposes)
"""
import io
import os
import boto3
import pandas as pd

# Setting up the variables for s3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET = os.getenv("BUCKET")
COMPRESSION = "gzip"
REGION_NAME = "EU-west-1"


def read_data_s3(file_path):
    """ Reads a compressed file in an s3 bucket

    Parameters:
    file_path (string): The path of the compressed csv in the bucket

    Returns:
    data_frame (DataFrame() from pandas): The data from the compressed file in the s3 bucket

    """
    session = boto3.session.Session(region_name=REGION_NAME)
    s3client = session.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    try:
        response = s3client.get_object(Bucket=BUCKET, Key=file_path)
    except s3client.exceptions.NoSuchKey as err:
        return print(err)

    data_frame = pd.read_csv(
        io.BytesIO(response["Body"].read()),
        sep=";",
        header=0,
        compression=COMPRESSION
    )

    return data_frame
