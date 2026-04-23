from sqlalchemy import create_engine
from urllib.parse import quote_plus
import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(env_path)

user = os.getenv("username")
password = os.getenv("password")
database = os.getenv("database")
host = os.getenv("host")


def get_engine():
    logging.info(f"Connecting to {host}:5432/{database}")
    return create_engine(
        f"postgresql+psycopg2://{user}:{quote_plus(password)}@{host}:5432/{database}"
    )


engine = get_engine()


def load_weather_data(table_name: str, df):
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',
        index=False,
    )

    logging.info("Loading data")

    df_check = pd.read_sql(f'select * from {table_name}', con=engine)
    logging.info(f"{len(df_check)} rows loaded")
