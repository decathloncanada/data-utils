"""
This module contains helper functions to import data from an s3 bucket to a .csv
(For refactoring purposes)
"""
import io
import os

import boto3
import tablib
import pandas as pd
import numpy as np

if os.getenv("DJANGO_SETTINGS_MODULE"):
    from import_export import resources

# Setting up the variables for s3
COMPRESSION = "gzip"
REGION_NAME = "EU-west-1"


class s3:
    def __init__(self, aws_key=None, secret_key=None, bucket=None):
        self.aws_key = aws_key
        self.secret_key = secret_key
        self.bucket = bucket

    def convert_s3_csv_to_df(self, key):
        # Connect to the s3 bucket and extract the compressed csv at the key
        session = boto3.session.Session(region_name=REGION_NAME)
        s3client = session.client(
            "s3",
            aws_access_key_id=self.aws_key,
            aws_secret_access_key=self.secret_key,
        )
        response = s3client.get_object(Bucket=self.bucket, Key=key)

        df = pd.read_csv(
            io.BytesIO(response["Body"].read()),
            sep=";",
            header=0,
            compression=COMPRESSION
        )

        return df

    def save_to_csv(self, df, filepath='./'):
        self.create_filepath_if_nonexistent(filepath)

        df.fillna(0.0, inplace=True)
        df.index = np.arange(1, len(df)+1)
        df.to_csv(filepath, index_label="id", sep=",", encoding="utf-8")

    def create_filepath_if_nonexistent(self, filepath):
        # If the resources directory doesn't exist, create it
        if not os.path.exists(os.path.dirname(filepath)):
            os.makedirs(os.path.dirname(filepath))

        # If the .csv file doesn't exist, create it
        if not os.path.exists(filepath):
            file = open(filepath, mode="w")
            file.close()

    def convert_df_to_django_model(self, df, model, rewrite=False):
        if not os.getenv("DJANGO_SETTINGS_MODULE"):
            raise Exception(
                "DJANGO_SETTINGS_MODULE is not set. This function is for Django projects."
            )

        if rewrite:
            self.clear_model_table(model)

        try:
            # Since Django's ORM uses incremental IDs by default
            # we need to go and take the next 'available' one
            # if the query returns none, then we start at 0
            query = model.objects.values('id').order_by('-id').first()
            last_id = query["id"] + 1 if query is not None else 0

            dataset = self.convert_df_to_dataset(df, last_id)

            # Save the data to the database
            p_resource = resources.modelresource_factory(model=model)()
            p_resource.import_data(dataset, dry_run=False)
        except Exception as err:
            return print(err)

    def convert_df_to_dataset(self, df, last_id):
        # Set the new ids to start with the next available one
        # Change the dataframe into a dictionnary
        # because you can't change it into a Dataset directly
        df["id"] = range(last_id, last_id + len(df))
        df.filna(0.0, inplace=True)
        headers = list(df)
        df = df.to_dict('records')

        # Put the Dataframe's data into a Dataset
        dataset = tablib.Dataset()
        dataset.headers = headers
        dataset.dict = df

        return dataset

    def clear_model_table(self, model):
        try:
            model.objects.all().delete()
        except Exception as err:
            return print(err)
