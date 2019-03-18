# Data-utils
In an effort to simplify the AI's team work, this repo will be used to seamlessly acquire the data from the databases.
For now, this module is for a specific use case where we retrieve data in csv format

## Usage
```py
import os
import pandas as pd
from s3.utils import s3

from most_popular_product import Product

client = s3(aws_key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            bucket=os.getenv("BUCKET"))

dataframe = client.read_data_s3(key='Dkt_canada/data/sport_popularity/city_sport_2019-03-13_000.gz')

client.save_to_csv(filepath='./sports.csv', dataframe=dataframe)

# Usage example in a django project
client.django_import_csv_to_model(filepath='./sports.csv', model=Product, rewrite=True)
```