import os
from datetime import datetime, timedelta

from dotenv import load_dotenv

import data_utils as du  # du for data_utils

load_dotenv()


YESTERDAY = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")

df = du.df.import_s3_csv_to_df(
    _aws_key=os.getenv("AWS_ACCESS_KEY_ID"),
    _secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    _bucket=os.getenv("BUCKET"),
    _region='eu-west-1',
    key=f'Dkt_canada/data/sport_popularity/city_sport_{YESTERDAY}_000.gz'
)


du.df.convert_df_to_csv(df, filepath='./sports.csv')
