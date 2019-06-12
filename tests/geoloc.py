import os
import unittest

from dotenv import load_dotenv
import boto3
import pandas as pd
from scipy import spatial

import data_utils.geoloc as du  # du for data_utils

load_dotenv()


class TestDfMethods(unittest.TestCase):

    def setUp(self):
        data = {'latitude': [45], 'longitude': [-73]}
        self.df = pd.DataFrame(data=data)
        self.kd_tree = du.map_df_to_kd_tree(self.df)

    def test_kd_tree_type(self):
        self.assertIsInstance(self.kd_tree, spatial.KDTree)

    def test_calculate_haversine_distance_type(self):
        distance = du.calculate_haversine_distance(self.df, 43, -73.56)
        self.assertEqual(type(distance), float)


if __name__ == '__main__':
    unittest.main(warnings='ignore')
