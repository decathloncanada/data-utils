# Data-utils
In an effort to simplify the AI's team work, this repo will be used to seamlessly acquire the data from the databases.

For now, this module is for a specific use case where we imports a compressed csv from an s3 bucket into a dataframe from pandas or csv format.

Can also be used alongside django projects to import data inside of a specified model's table.

## Usage Example
```py
import os
from datetime import datetime, timedelta

from s3.utils import s3

YESTERDAY = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")

client = s3(
    aws_key=os.getenv("AWS_ACCESS_KEY_ID"),
    secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    bucket=os.getenv("BUCKET")
)

df = client.read_data_s3(key=f'Dkt_canada/data/Product{YESTERDAY}.gz')

# Usage example in a django project
client.convert_df_to_django_model(df=df, model=Product, rewrite=True)
```