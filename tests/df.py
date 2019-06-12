import os
import unittest

from dotenv import load_dotenv
import boto3
import pandas as pd

import data_utils.df as du  # du for data_utils

load_dotenv()


class TestDfMethods(unittest.TestCase):

    def setUp(self):
        # Connect to the s3 bucket and extract the compressed csv at the key
        session = boto3.session.Session(region_name='eu-west-1')
        self.s3client = session.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )

        data = {'col1': [1, 2, 3, 4, 5], 'col2': [1, 2, 3, 4, 5]}
        df = pd.DataFrame(data=data)

        du.convert_df_to_s3_compressed_csv(df,
                                           s3client=self.s3client,
                                           bucket=os.getenv("BUCKET"),
                                           key=f'Dkt_canada/shawn_test/test_000.gz')

        self.df = du.import_s3_csv_to_df(s3client=self.s3client,
                                         bucket=os.getenv("BUCKET"),
                                         key=f'Dkt_canada/shawn_test/test_000.gz')

        self.list_keys = du.list_s3_keys_in_bucket(self.s3client,
                                                   bucket=os.getenv("BUCKET"),
                                                   prefix=f'Dkt_canada/shawn_test/')

    def test_check_df_type(self):
        self.assertEqual(type(self.df), pd.DataFrame)

    def test_count_df_rows(self):
        self.assertEqual(self.df.shape[0], 5)

    def test_count_df_columns(self):
        self.assertEqual(self.df.shape[1], 2)

    def test_check_list_type(self):
        self.assertEqual(type(self.list_keys), list)

    def test_count_list_keys(self):
        self.assertEqual(len(self.list_keys), 1)

    def test_convert_df_to_s3_compressed_csv(self):
        self.assertTrue('Dkt_canada/shawn_test/test_000.gz' in self.list_keys)


if __name__ == '__main__':
    unittest.main(warnings='ignore')
