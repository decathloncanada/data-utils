import os

import tablib
import numpy as np


def _get_x(phi, theta):
    return 6367 * np.cos(phi) * np.cos(theta)


def _get_y(phi, theta):
    return 6367 * np.cos(phi) * np.sin(theta)


def _get_z(phi):
    return 6367 * np.sin(phi)


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
