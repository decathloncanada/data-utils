# Data-utils
In an effort to simplify the AI's team work, this repo will be used to seamlessly acquire the data from the databases.
For now, this module is for a specific use case where we retrieve data in csv format

## Usage
```py
import os
import pandas as pd
from s3.utils import s3

client = s3(aws_key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            bucket=os.getenv("BUCKET"))

dataframe = client.read_data_s3('Dkt_canada/data/sport_popularity/city_sport_2019-03-13_000.gz')
client.save_to_csv('./sports.csv', dataframe)
```

#### This is what I envision the second version to be
```py
from data_utils import s3

# It will require those 3 variables to create a connection to the bucket on s3
s3.setup(aws_key=os.getenv('AWS_KEY'), secret_key=os.getenv('SECRET_KEY'), bucket=os.getenv('BUCKET'))

# extract_to_csv will have the compression set to 'gzip' by default
s3.extract_to_csv(key='put/in/your/link', path='where/to/save/the/file')
```