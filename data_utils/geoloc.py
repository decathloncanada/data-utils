# -*- coding: utf-8 -*-

"""
data_utils.geoloc
~~~~~~~~~~~~~
This module contains the functions to aid in the geolocation features.
"""
from math import radians, cos, sin, asin, sqrt

import numpy as np
import scipy.spatial as spatial

from .utils import(
    _get_x,
    _get_y,
    _get_z
)


def map_df_to_kd_tree(df):
    """
    Maps a dataframe with a latitude and longitude to a KDTree from scipy.spatial
    This provides quick access to the nearest-neighbour lookup.

    :df: pandas.DataFrame with required latitude and longitude

    This function returns a KDTree based on the dataframe
    """
    phi = np.deg2rad(df['latitude'])
    theta = np.deg2rad(df['longitude'])
    df['x'] = _get_x(phi, theta)
    df['y'] = _get_y(phi, theta)
    df['z'] = _get_z(phi)

    return spatial.KDTree(df[['x', 'y', 'z']])


def calculate_haversine_distance(df, latitude, longitude):
    """
    Calculate the great circle distance between two points
    on Earth (specified in decimal degrees)

    :df: pandas.DataFrame with required latitude and longitude. It is the 'point of origin'
    :latitude: float representing the respective degree of the location
    :longitude: float representing the respective degree of the location

    Usage: df['distance'] = df.apply(distance_haversine, args=(latitude, longitude), axis=1)

    Haversine formula:
        a = sin²(Δφ/2) + cos φ1 ⋅ cos φ2 ⋅ sin²(Δλ/2)
        c = 2 ⋅ atan2( √a, √(1−a) )
        d = R ⋅ c

    where φ is latitude, λ is longitude, R is earth’s radius (mean radius = 6,371km);
            note that angles need to be in radians to pass to trig functions!

    """
    R = 6371.0  # earths's radius in km

    # convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(
        radians, [df['latitude'], df['longitude'], latitude, longitude]
    )
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    # haversine formula
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))  # 2 * atan2(sqrt(a), sqrt(1-a))
    d = R * c
    return d
