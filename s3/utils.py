"""
This module contains helper functions to import data from an s3 bucket to a .csv
(For refactoring purposes)
"""
import io
import os
import boto3
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

    def read_data_s3(self, key=None):
        """ Reads a compressed file in an s3 bucket

        Parameters:
        key (string): The path of the compressed csv in the bucket

        Returns:
        dataframe (DataFrame() from pandas): The data from the compressed file in the s3 bucket

        """
        session = boto3.session.Session(region_name=REGION_NAME)
        s3client = session.client(
            "s3",
            aws_access_key_id=self.aws_key,
            aws_secret_access_key=self.secret_key,
        )

        try:
            response = s3client.get_object(Bucket=self.bucket, Key=key)
        except s3client.exceptions.NoSuchKey as err:
            return print(err)

        dataframe = pd.read_csv(
            io.BytesIO(response["Body"].read()),
            sep=";",
            header=0,
            compression=COMPRESSION
        )

        return dataframe

    def save_to_csv(self, filepath='./', dataframe=None):
        """ Saves the dataframe to a specified filepath

        Parameters:
        filepath (string), default='./': The path where to save the .csv to
        dataframe (DataFrame() from pandas): The data frame with all the product's information

        """
        # If the resources directory doesn't exist, create it
        if not os.path.exists(os.path.dirname(filepath)):
            os.makedirs(os.path.dirname(filepath))

        # If the .csv file doesn't exist, create it
        if not os.path.exists(filepath):
            file = open(filepath, mode="w")
            file.close()

        mdata = dataframe.fillna(0.0)

        mdata.index = np.arange(1, len(mdata) + 1)
        mdata.to_csv(filepath, index_label="id", sep=",", encoding="utf-8")


def django_import_csv_to_model(filepath=None, model=None, rewrite=False):
    """ Writes the file at filepath to the model in the db

    Parameters:
    filepath (string): The path of the .csv file in ressources
    model (Class): The model for the data to be set to
    rewrite (bool), default=False: if True, it will delete the entries of the model in the db

    """

    if not os.getenv("DJANGO_SETTINGS_MODULE"):
        raise Exception("DJANGO_SETTINGS_MODULE is not set. This function is for Django projects.")
    # Verify it can delete the entries in the db
    if rewrite:
        try:
            model.objects.all().delete()
        except Exception as err:
            return print(err)

    # Verify it can open the csv
    try:
        with open(filepath) as csvfile:
            reader = csv.reader(csvfile, delimiter=",")
            headers = next(reader, None)
            for row in reader:
                p_resource = resources.modelresource_factory(model=model)()
                dataset = tablib.Dataset(row, headers=headers)
                p_resource.import_data(dataset, dry_run=False)
    except Exception as err:
        return print(err)
