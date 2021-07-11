import os
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import seaborn as sb

DB_NAME = os.environ['MYSQL_DATABASE']

config = {
  'user': os.environ['MYSQL_USER'],
  'password': os.environ['MYSQL_PASSWORD'],
  'host': os.environ['db_endpoint'],
  'database': os.environ['MYSQL_DATABASE'],
  'raise_on_warnings': True
}

engine_str = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:3306/{config['database']}"

engine = create_engine(engine_str, echo=False)

def load_diamonds_to_db():
    df = sb.load_dataset('diamonds')
    with engine.connect() as conn:
        df.to_sql("diamonds_org",conn,if_exists='replace',schema=DB_NAME)

