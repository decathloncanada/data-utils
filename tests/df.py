import os
import unittest

from dotenv import load_dotenv
import boto3
import pandas as pd

import data_utils.df as du  # du for data_utils

load_dotenv()

S3CLIENT = None


def setUp():
    global S3CLIENT
    # Connect to the s3 bucket and extract the compressed csv at the key
    session = boto3.session.Session(region_name='eu-west-1')
    S3CLIENT = session.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    data = {'col1': [1, 2, 3, 4, 5], 'col2': [1, 2, 3, 4, 5]}
    df = pd.DataFrame(data=data)

    du.convert_df_to_s3_compressed_csv(df,
                                       s3client=S3CLIENT,
                                       bucket=os.getenv("BUCKET"),
                                       key=f'test/test.gz')


class TestImportS3CsvToDf(unittest.TestCase):

    def setUp(self):
        self.df = du.import_s3_csv_to_df(s3client=S3CLIENT,
                                         bucket=os.getenv("BUCKET"),
                                         key=f'test/test.gz')

    def test_check_df_type(self):
        self.assertEqual(type(self.df), pd.DataFrame)

    def test_count_df_rows(self):
        self.assertEqual(self.df.shape[0], 5)

    def test_count_df_columns(self):
        self.assertEqual(self.df.shape[1], 2)


class TestListS3KeysInBucket(unittest.TestCase):

    def setUp(self):
        self.list_keys = du.list_s3_keys_in_bucket(S3CLIENT,
                                                   bucket=os.getenv("BUCKET"),
                                                   prefix=f'test/')

    def test_check_list_type(self):
        self.assertEqual(type(self.list_keys), list)

    def test_count_list_keys(self):
        self.assertEqual(len(self.list_keys), 1)

    def test_convert_df_to_s3_compressed_csv(self):
        self.assertTrue('test/test.gz' in self.list_keys)


if __name__ == '__main__':
    setUp()
    unittest.main(warnings='ignore')
