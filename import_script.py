import os
from s3.utils import s3
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(dotenv_path=ENV_PATH)

client = s3(aws_key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            bucket=os.getenv("BUCKET"))

dataframe = client.read_data_s3('Dkt_canada/data/sport_popularity/city_sport_2019-03-13_000.gz')

print(dataframe)

# client.save_to_csv('./sports.csv', dataframe)