from math import radians, cos, sin, asin, sqrt

import numpy as np
import scipy.spatial as spatial


def _get_x(phi, theta):
    return 6367 * np.cos(phi) * np.cos(theta)


def _get_y(phi, theta):
    return 6367 * np.cos(phi) * np.sin(theta)


def _get_z(phi):
    return 6367 * np.sin(phi)


def map_df_to_kd_tree(df):
    # method to map the dataframe to a kdtree, for faster search
    """
    the dataframe needs a latitude and a longitude column
    """
    phi = np.deg2rad(df['latitude'])
    theta = np.deg2rad(df['longitude'])
    df['x'] = _get_x(phi, theta)
    df['y'] = _get_y(phi, theta)
    df['z'] = _get_z(phi)
    tree = spatial.KDTree(df[['x', 'y', 'z']])

    return tree


def calculate_haversine_distance(df, latitude, longitude):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    Usage:
        df['distance'] = df.apply(distance_haversine, args=(latitude, longitude), axis=1)

    Haversine formula:
        a = sin²(Δφ/2) + cos φ1 ⋅ cos φ2 ⋅ sin²(Δλ/2)
        c = 2 ⋅ atan2( √a, √(1−a) )
        d = R ⋅ c

    where φ is latitude, λ is longitude, R is earth’s radius (mean radius = 6,371km);
            note that angles need to be in radians to pass to trig functions!

    """
    R = 6371.0  # earths's radius in km

    # convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [df['latitude'], df['longitude'], latitude, longitude])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))  # 2 * atan2(sqrt(a), sqrt(1-a))
    d = R * c
    return d
