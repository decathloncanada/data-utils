"""
This module contains helper functions to import data from an s3 bucket to a .csv
(For refactoring purposes)
"""
import io
import os
import boto3
import pandas as pd
import numpy as np

# Setting up the variables for s3
COMPRESSION = "gzip"
REGION_NAME = "EU-west-1"


class s3:
    def __init__(self, aws_key, secret_key, bucket):
        self.aws_key = aws_key
        self.secret_key = secret_key
        self.bucket = bucket

    def read_data_s3(self, file_path):
        """ Reads a compressed file in an s3 bucket

        Parameters:
        file_path (string): The path of the compressed csv in the bucket

        Returns:
        data_frame (DataFrame() from pandas): The data from the compressed file in the s3 bucket

        """
        session = boto3.session.Session(region_name=REGION_NAME)
        s3client = session.client(
            "s3",
            aws_access_key_id=self.aws_key,
            aws_secret_access_key=self.secret_key,
        )

        try:
            response = s3client.get_object(Bucket=self.bucket, Key=file_path)
        except s3client.exceptions.NoSuchKey as err:
            return print(err)

        data_frame = pd.read_csv(
            io.BytesIO(response["Body"].read()),
            sep=";",
            header=0,
            compression=COMPRESSION
        )

        return data_frame

    def save_to_resources(self, filepath, data_frame):
        """ Saves the data_frame to a specified filepath

        Parameters:
        filepath (string): The path where to save the .csv to
        data_frame (DataFrame from pandas): The data frame with all the product's information

        """
        # If the resources directory doesn't exist, create it
        if not os.path.exists(os.path.dirname(filepath)):
            os.makedirs(os.path.dirname(filepath))

        # If the .csv file doesn't exist, create it
        if not os.path.exists(filepath):
            file = open(filepath, mode="w")
            file.close()

        mdata = data_frame.fillna(0.0)

        mdata.index = np.arange(1, len(mdata) + 1)
        mdata.to_csv(filepath, index_label="id", sep=",", encoding="utf-8")
