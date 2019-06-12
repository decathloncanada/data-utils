import unittest

from dotenv import load_dotenv
import pandas as pd
from scipy import spatial

import data_utils.geoloc as du  # du for data_utils

load_dotenv()


class TestKDTree(unittest.TestCase):

    def setUp(self):
        data = {'latitude': [45], 'longitude': [-73]}
        df = pd.DataFrame(data=data)
        self.kd_tree = du.map_df_to_kd_tree(df)

    def test_type(self):
        self.assertIsInstance(self.kd_tree, spatial.KDTree)


class TestCalculateHaversineDistance(unittest.TestCase):

    def setUp(self):
        data = {'latitude': [45], 'longitude': [-73]}
        df = pd.DataFrame(data=data)
        self.exact_location = du.calculate_haversine_distance(df, 45, -73)

    def test_type(self):
        self.assertEqual(type(self.exact_location), float)

    def test_same_point_of_origin(self):
        self.assertEqual(self.exact_location, 0.0)


if __name__ == '__main__':
    unittest.main(warnings='ignore')
