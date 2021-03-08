# -*- coding: utf-8 -*-

"""
data_utils.df
~~~~~~~~~~~~~
This module contains the functions related to dataframe manipulation.
"""
import os
import io

import pandas as pd
import numpy as np
import tablib

from .utils import (_clear_model_table,
                    _convert_df_to_dataset,
                    _create_filepath_if_nonexistent)


def import_s3_csv_to_df(s3client,
                        bucket,
                        key,
                        sep=';',
                        header=0,
                        compression='gzip',
                        usecols=None,
                        dtype=None,
                        error_bad_lines=False,
                        drop_id=True):
    """
    Returns a dataframe based on thecompressed csv at the given key in the given bucket

    :s3client: boto3.session.Session.client that represents a connection with s3
    :bucket: string representing the s3 bucket's name
    :key: string representing the filepath in the s3 bucket
    :sep: string representing the seperation in the compressed csv, default: ';'
    :header: row number to use as the column names, default: 0
    :compression: string representing the type of compression on the file, default: 'gzip'
    :dtype: dictionary {attribute name: object type}
    :usecols: list of attributes to read from the csv
    """
    response = s3client.get_object(Bucket=bucket, Key=key)

    df = pd.read_csv(io.BytesIO(response['Body'].read()),
                     sep=sep,
                     header=header,
                     compression=compression,
                     dtype=dtype,
                     usecols=usecols,
                     error_bad_lines= error_bad_lines)

    # drop duplicate to fix
    # duplicate 'id' column in the df
    try:
        if drop_id:
          df.drop('id', inplace=True, axis=1)
    except KeyError:
        pass

    return df


def list_s3_keys_in_bucket(s3client,
                           bucket,
                           prefix=''):
    """
    Returns a list of the keys situated at the given prefix in the given bucket

    :s3client: boto3.session.Session.client that represents a connection with s3
    :bucket: string representing the s3 bucket's name
    :prefix: string representing the base filepath to search at in the s3 bucket, default: ''
    """
    keys = []
    response = s3client.list_objects(Bucket=bucket, Prefix=prefix)['Contents']
    for csv in response:
        keys.append(csv['Key'])

    return keys


def convert_df_to_s3_compressed_csv(df,
                                    s3client,
                                    bucket,
                                    key,
                                    sep=';',
                                    compression='gzip'):
    """
    Receives a dataframe and compress it into a csv
    to the put it in the bucket at the key

    :df: pandas.DataFrame to convert into a compressed csv
    :s3client: boto3.session.Session.client that represents a connection with s3
    :bucket: string representing the s3 bucket's name
    :key: string representing the filepath in the s3 bucket
    :sep: string representing the seperation in the compressed csv, default: ';'
    :compression: string representing the type of compression on the file, default: 'gzip'
    """
    tmp_file = './tmp_gzip_csv'

    convert_df_to_csv(df, filepath=tmp_file, sep=sep, compression=compression)

    s3client.upload_file(Filename=tmp_file,
                         Bucket=bucket,
                         Key=key)

    os.remove('./tmp_gzip_csv')


def convert_df_to_csv(df, filepath, index_label='id', sep=',', encoding='utf-8', compression=None):
    """
    Convert a given dataframe to a csv at the filepath using
    the other arguments sa specifications

    :df: pandas.Dataframe to convert
    :filepath: string representing what path to save the csv to
    :index_label: string representing the column label for the index column, default: 'id'
    :sep: string representing the wanted seperation in the csv, default: ','
    :encoding: string representing the encoding to use in the output file, default: 'utf-8'
    """
    _create_filepath_if_nonexistent(filepath)

    df.fillna(0.0, inplace=True)
    df.index = np.arange(1, len(df)+1)
    df.to_csv(filepath,
              index_label=index_label,
              sep=sep,
              encoding=encoding,
              compression=compression)


def convert_df_to_django_model(df,
                               model,
                               rewrite=False,
                               rows_at_a_time=250):
    """
    Import a given dataframe to Django's ORM with a specified model

    :df: pandas.Dataframe to convert
    :model: django.db.models.Model's name. The ORM takes care of which table to put the data in
    :rewrite: boolean representing wether to delete the old entries or not, default: False
    :rows_at_a_time: int representing the amount of rows to import at the same time, default: 250
    """
    if os.getenv('DJANGO_SETTINGS_MODULE'):
        from import_export import resources
    else:
        raise Exception('This function can only be used in Django projects.')

    if rewrite:
        _clear_model_table(model)

    try:
        # Since Django's ORM uses incremental IDs by default
        # we need to go and take the next 'available' one
        # if the query returns none, then we start at 0
        query = model.objects.values('id').order_by('-id').first()
        last_id = query['id'] + 1 if query is not None else 0

        dataset = _convert_df_to_dataset(df, last_id)

        p_resource = resources.modelresource_factory(model=model)()
        for i in range(0, len(dataset), rows_at_a_time):
            data = tablib.Dataset(*dataset[i:i+rows_at_a_time],
                                  headers=dataset.headers)
            p_resource.import_data(data)
    except Exception as err:
        return print(err)
