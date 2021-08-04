
import os, requests, logging, json, datetime
from sqlalchemy import create_engine, MetaData, Table, Column, Float, Integer, DateTime, String, sql
import pandas as pd
import seaborn as sb
import sqlalchemy
from sqlalchemy.sql.expression import table

DB_NAME = os.environ['MYSQL_DATABASE']
DIAMONDS_WEB_SOURCE = os.environ['DIAMONDS_WEB_SOURCE']

config = {
    'user': os.environ['MYSQL_USER'],
    'password': os.environ['MYSQL_PASSWORD'],
    'host': os.environ['db_endpoint'],
    'database': os.environ['MYSQL_DATABASE'],
    'raise_on_warnings': True
}
engine_str = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:3306/{config['database']}"
engine = create_engine(engine_str, echo=False, pool_recycle=3600)
metadata = MetaData()

prediction_audit_table = Table('prediction_audit', metadata,
    Column('audit_id',Integer, primary_key=True, autoincrement=True),
    Column('record_time', DateTime, server_default=sql.func.now()),
    Column('carat', Float),
    Column('depth', Float),
    Column('table', Float),
    Column('cut', String(10)),
    Column('color', String(10)),
    Column('clarity', String(10)),
    Column('x', Float),
    Column('y', Float),
    Column('z', Float),
    Column('predicted_price', Float),
    Column('user_satisfaction', Integer),
    Column('user_suggested_price', Float))

alerts_table = Table('alerts_table', metadata,
    Column('alert_id', Integer, primary_key=True, autoincrement=True),
    Column('record_time', DateTime, server_default=sql.func.now()),
    Column('alert_sevirity', String(10)),
    Column('alert_type', String(50)),
    Column('alert_text',String(1000))
    )

outsource_diamonds = Table('outsource_diamonds', metadata,
    Column('index', Integer, primary_key=True, autoincrement=False),
    Column('record_time', DateTime, server_default=sql.func.now()),
    Column('carat', Float),
    Column('cut', String(50)),
    Column('color',String(50)),
    Column('clarity',String(50)),
    Column('depth', Float),
    Column('table', Float),
    Column('price', Float),
    Column('x', Float),
    Column('y', Float),
    Column('z', Float)
    )

diamonds_outliers = Table('diamonds_outliers', metadata,
    Column('index', Integer, primary_key=True, autoincrement=False),
    Column('record_time', DateTime, server_default=sql.func.now()),
    Column('carat', Float),
    Column('cut', String(50)),
    Column('color',String(50)),
    Column('clarity',String(50)),
    Column('depth', Float),
    Column('table', Float),
    Column('price', Float),
    Column('x', Float),
    Column('y', Float),
    Column('z', Float)
    )

def init_tables():  
    with engine.connect() as conn:
        metadata.create_all(engine)


def load_data_from_web():
    data = requests.post(DIAMONDS_WEB_SOURCE)
    df = pd.read_json(data.text)
    with engine.connect() as conn:
        for index, row in df.iterrows():
            row_data = json.loads(row.to_json())
            insert_stmt = \
            sqlalchemy.dialects.mysql.insert(outsource_diamonds).values(row_data)
            insert_stmt = insert_stmt.on_duplicate_key_update(row_data)
                
            result = conn.execute(insert_stmt)  

    add_alert({'alert_sevirity':'INFO',
    'alert_type':'LOAD_DATA_FROM_WEB',
    'alert_text':f'Downloaded new {df.shape[0]:,d} rows from web service'}) 

def load_diamonds_to_db():
    df = sb.load_dataset('diamonds')
    with engine.connect() as conn:
        df.to_sql("diamonds_org",conn,if_exists='replace',schema=DB_NAME)

def read_table_from_db(table_name,limit=None, order_by=None, order_asc_desc=None, rand=None):
    query = f'SELECT * FROM {DB_NAME}.{table_name}'
    if rand is not None:
        query = query + f' ORDER BY RAND() LIMIT {rand}'
    else:
        if order_by is not None:
            query = query + f' ORDER BY {order_by}'
        if order_asc_desc is not None:
            query = query + f' {order_asc_desc}'
        if limit is not None:
            query = query + f' LIMIT {limit}'

    size_query = f'SELECT COUNT(*) FROM {DB_NAME}.{table_name}'

    with engine.connect() as conn:
        df = pd.read_sql(query,conn)
        df2 = pd.read_sql(size_query, conn)

    return {'df':df.to_json(), 'size':df2.to_json()}

def audit_predition(row:dict) -> int:
    insert_stmt = prediction_audit_table.insert().values(carat = row['carat'],
        depth = row['depth'], table=row['table'], cut=row['cut'], color = row['color'], 
        clarity = row['clarity'], x = row['x'], y=row['y'], z=row['z'], predicted_price=row['predicted_price'])
   
    with engine.connect() as conn:
     result = conn.execute(insert_stmt)
    
    return result.inserted_primary_key[0]

def submit_feedback(row_id, grade, user_prediction) -> None:
    update_stmt = prediction_audit_table.update().\
        where(prediction_audit_table.c.audit_id==row_id).\
            values(user_satisfaction=int(grade), user_suggested_price=float(user_prediction.replace(',','')))
    
    with engine.connect() as conn:
        result = conn.execute(update_stmt)

def store_outliers(df: pd.DataFrame):
    df['record_time']=\
        df['record_time'].apply(lambda x:str(datetime.datetime.fromtimestamp(x.value/(10**9))))
    # df['record_time'] = df['record_time'].values.astype('datetime64[us]')
    # df['record_time'] = df['record_time'].to_timestamp()
    
    # df['record_time'] = pd.datetime(df['record_time'], unit='ms')

    # df['record_time'] = df['record_time'].values.astype('datetime64[us]')
    with engine.connect() as conn:
        for index, row in df.iterrows():
            row_data = json.loads(row.to_json())
            insert_stmt = \
            sqlalchemy.dialects.mysql.insert(diamonds_outliers).values(row_data)
            insert_stmt = insert_stmt.on_duplicate_key_update(row_data)
                
            result = conn.execute(insert_stmt)  

    add_alert({'alert_sevirity':'INFO',
    'alert_type':'OUTLIERS_ADDED',
    'alert_text':f'Added {df.shape[0]:,d} rows to outliers table'}) 

def add_alert(data) -> int:
    insert_stmt = alerts_table.insert().values(
        alert_sevirity=data['alert_sevirity'], alert_type=data['alert_type'], alert_text=data['alert_text']
    )

    with engine.connect() as conn:
        result = conn.execute(insert_stmt)

    return result.inserted_primary_key[0]