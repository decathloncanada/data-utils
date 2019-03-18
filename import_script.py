import os
from s3.utils import s3
from dotenv import load_dotenv
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(dotenv_path=ENV_PATH)

YESTERDAY= (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")

client = s3(aws_key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            bucket=os.getenv("BUCKET"))

dataframe = client.read_data_s3('Dkt_canada/data/sport_popularity/city_sport_%s_000.gz' % (YESTERDAY))

client.save_to_csv('./sports.csv', dataframe)