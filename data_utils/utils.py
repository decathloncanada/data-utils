import io
import os

import boto3
import tablib
import pandas as pd
import numpy as np


def import_s3_csv_to_df(
        _aws_key,
        _secret_key,
        _bucket,
        _region,
        key,
        sep=';',
        header=0,
        compression='gzip'
):
    # Connect to the s3 bucket and extract the compressed csv at the key
    session = boto3.session.Session(region_name=_region)
    s3client = session.client(
        's3',
        aws_access_key_id=_aws_key,
        aws_secret_access_key=_secret_key,
    )
    response = s3client.get_object(Bucket=_bucket, Key=key)

    df = pd.read_csv(
        io.BytesIO(response['Body'].read()),
        sep=sep,
        header=header,
        compression=compression
    )

    return df


def convert_df_to_csv(df, filepath='./', index_label='id', sep=',', encoding='utf-8'):
    _create_filepath_if_nonexistent(filepath)

    df.fillna(0.0, inplace=True)
    df.index = np.arange(1, len(df)+1)
    df.to_csv(
        filepath,
        index_label=index_label,
        sep=sep,
        encoding=encoding
    )


def convert_df_to_django_model(df, model, rewrite=False):
    _setup_django()

    if rewrite:
        _clear_model_table(model)

    try:
        # Since Django's ORM uses incremental IDs by default
        # we need to go and take the next 'available' one
        # if the query returns none, then we start at 0
        query = model.objects.values('id').order_by('-id').first()
        last_id = query['id'] + 1 if query is not None else 0

        dataset = _convert_df_to_dataset(df, last_id)

        # Save the data to the database
        p_resource = resources.modelresource_factory(model=model)()
        p_resource.import_data(dataset)
    except Exception as err:
        return print(err)


def _convert_df_to_dataset(df, last_id):
    # Set the new ids to start with the next available one
    # Change the dataframe into a dictionnary
    # because you can't change it into a Dataset directly
    df['id'] = range(last_id, last_id + len(df))
    df.fillna(0.0, inplace=True)
    headers = list(df)
    df = df.to_dict('records')

    # Put the Dataframe's data into a Dataset
    dataset = tablib.Dataset()
    dataset.headers = headers
    dataset.dict = df

    return dataset


def _clear_model_table(model):
    try:
        model.objects.all().delete()
    except Exception as err:
        return print(err)


def _create_filepath_if_nonexistent(filepath):
    # If the directory doesn't exist, create it
    if not os.path.exists(os.path.dirname(filepath)):
        os.makedirs(os.path.dirname(filepath))

    # If the file doesn't exist, create it
    if not os.path.exists(filepath):
        file = open(filepath, mode='w')
        file.close()


def _setup_django():
    if os.getenv('DJANGO_SETTINGS_MODULE'):
        from import_export import resources
    else:
        raise Exception('This function can only be used in Django projects.')
