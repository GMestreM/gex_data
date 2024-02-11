"""Functions to connect and retrieve data from databases"""

import os 
from io import BytesIO, StringIO, TextIOWrapper
import gzip
import datetime
import requests
from dotenv import load_dotenv
import pandas as pd
import yfinance as yf

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient
from bson.objectid import ObjectId
import cloudinary
import cloudinary.uploader

from cboe_data import get_quotes, get_ticker_info
from gamma_exposure import calculate_gamma_profile, calculate_spot_total_gamma_call_puts
from config import CONFIG

# Get env variables
load_dotenv()

# Create engine for PostgreSQL
# ============================
PG_USER_NAME = os.environ.get('PG_USER_NAME','Unable to retrieve PG_USER_NAME')
PG_USER_PWD = os.environ.get('PG_USER_PWD','Unable to retrieve PG_USER_PWD')
PG_REF_ID = os.environ.get('PG_REF_ID','Unable to retrieve PG_REF_ID')
PG_REGION = os.environ.get('PG_REGION','Unable to retrieve PG_REGION')
PG_URL_PORT = os.environ.get('PG_URL_PORT','Unable to retrieve PG_URL_PORT')

pool_connection = f"postgresql://{PG_USER_NAME}.{PG_REF_ID}:{PG_USER_PWD}@{PG_REGION}:{PG_URL_PORT}/postgres"
engine = create_engine(pool_connection,pool_pre_ping=True, pool_size=15)
engine.connect()

Base = declarative_base()

# Create a session to interact with the database
SessionLocal = sessionmaker(autocommit=False,autoflush=False,bind=engine)
session = SessionLocal()


# Create connection for MongoDB
# =============================
MONGO_USER = os.environ.get('MONGO_USER','Unable to retrieve MONGO_USER')
MONGO_PWD = os.environ.get('MONGO_PWD','Unable to retrieve MONGO_PWD')
MONGO_DB_URL = os.environ.get('MONGO_DB_URL','Unable to retrieve MONGO_DB_URL')
MONGO_DB_NAME = os.environ.get('MONGO_DB_NAME','Unable to retrieve MONGO_DB_NAME')

mongo_url = f"mongodb+srv://{MONGO_USER}:{MONGO_PWD}@{MONGO_DB_URL}/?retryWrites=true&w=majority"


# Create connection for Cloudinary
# ================================
CLOUDINARY_API_KEY = os.environ.get('CLOUDINARY_API_KEY','Unable to retrieve CLOUDINARY_API_KEY')
CLOUDINARY_API_SECRET = os.environ.get('CLOUDINARY_API_SECRET','Unable to retrieve CLOUDINARY_API_SECRET')
CLOUDINARY_CLOUD_NAME = os.environ.get('CLOUDINARY_CLOUD_NAME','Unable to retrieve CLOUDINARY_CLOUD_NAME')

cloudinary.config( 
  cloud_name = CLOUDINARY_CLOUD_NAME, 
  api_key = CLOUDINARY_API_KEY, 
  api_secret = CLOUDINARY_API_SECRET,
  secure=True,
)


def store_raw_option_chains() -> dict:
    """
    Retrieve (delayed) option chains from CBOE and store them
    """
    
    # Fetch data from CBOE
    option_chain = get_quotes(symbol=CONFIG['CBOE_TICKER'])
    option_chain['expiration'] = pd.to_datetime(option_chain['expiration'], format='%Y-%m-%d')
    # Get timestamp when query was performed
    query_timestamp = datetime.datetime.now(datetime.timezone.utc).astimezone()
    
    # Get additional ticker info
    option_chain_ticker_info = get_ticker_info(symbol=CONFIG['CBOE_TICKER'])[0]
    delayed_timestamp = option_chain_ticker_info.loc['lastTradeTimestamp',:].item()
    
    # Store compressed .tag.gz file in object storage
    fname_decompressed = f'df_test_memory.csv'
    fname_cloudinary = f'cboe_opt_chain_timestamp_{query_timestamp.strftime("%Y-%m-%dT%H:%M:%S%z")}_delayed_{delayed_timestamp}.tar.gz'


    with BytesIO() as buf:
        with gzip.GzipFile(fileobj=buf, mode='w') as gz_file:
            # gz_file.write(df_upload)
            option_chain.to_csv(TextIOWrapper(gz_file, 'utf8'), index=False, header=True)

        # Upload file to cloudinary
        response_compressed = cloudinary.uploader.upload(
            file=buf.getvalue(), 
            public_id=fname_cloudinary, # 'id_name'
            unique_filename = False, 
            overwrite=True,
            resource_type='raw',
            tags=CONFIG['CLOUDINARY_TAG'],   #'gamma_exposure'
            # context='timestamp=2024-01-25|test=True', #pipe-separated values
        )
        
    # Append additional info
    response_compressed['query_timestamp'] = query_timestamp.strftime("%Y-%m-%dT%H:%M:%S%z")
    response_compressed['delayed_timestamp'] = delayed_timestamp

    # Create MongoDB document with upload information
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_UPLOADS']]
        
        # Insert execution details in mongo
        upload_id = mongo_collection.insert_one(response_compressed).inserted_id
        
        # Add post_id info to dict
        response_compressed['mongodb_upload_id'] = str(upload_id)
        
        # Insert quote info in mongo
        dict_option_chain_ticker_info = option_chain_ticker_info.to_dict()[CONFIG['CBOE_TICKER']]
        dict_option_chain_ticker_info['ticker'] = CONFIG['CBOE_TICKER']
        dict_option_chain_ticker_info['mongodb_upload_id'] = response_compressed['mongodb_upload_id']
        
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_QUOTE_INFO']]
        info_id = mongo_collection.insert_one(dict_option_chain_ticker_info).inserted_id
        

    # Return dict with upload id
    return response_compressed

def store_execution_details_sql(response_compressed:dict) -> int:
    from data_models import IdTable
    # Create new instance of IdTable
    id_table = IdTable(
        execution_timestamp=pd.to_datetime(response_compressed['query_timestamp']),
        delayed_timestamp=pd.to_datetime(response_compressed['delayed_timestamp']),
        raw_data=response_compressed['secure_url'],
        mongodb_id=str(response_compressed['_id']),
    )
    
    # Write to sql
    session.add(id_table)
    session.commit()
    
    # Fetch id of new record
    result = session.query(IdTable).filter(IdTable.mongodb_id == str(response_compressed['_id']))
    df_id = pd.read_sql(result.statement,session.bind)
    
    id_sql = df_id['id'].item()
    
    return id_sql
        
    
def store_gamma_profile(secure_url:str, spot_price:float, last_trade_date:pd.Timestamp, mongodb_upload_id:str):
    
    # Retrieve option chain from url
    option_chain_long = get_df_from_storage(secure_url=secure_url)
    option_chain_long['expiration'] = pd.to_datetime(option_chain_long['expiration'])
    
    gex_profile, zero_gamma = calculate_gamma_profile(
        option_chain_long=option_chain_long, 
        spot_price=spot_price, 
        last_trade_date=last_trade_date,
        pct_from=0.8, 
        pct_to=1.2)
    
    df_zero_gamma = pd.DataFrame({'Zero Gamma':[zero_gamma]}, index=[last_trade_date])
    
    # Store it in mongodb
    mongo_doc = {
        mongodb_upload_id:gex_profile.reset_index().to_dict('list')
    }
    
    mongo_doc_zero_gamma = {
        mongodb_upload_id:df_zero_gamma.reset_index().to_dict('list')
    }
    # Fix date format
    mongo_doc_zero_gamma[mongodb_upload_id]['index'] = [last_trade_date]
    
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_GEX_PROFILE']]
        
        # Insert execution details in mongo
        upload_id = mongo_collection.insert_one(mongo_doc).inserted_id
        
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_GEX_ZERO_GAMMA']]
        
        # Insert execution details in mongo
        upload_id_zero = mongo_collection.insert_one(mongo_doc_zero_gamma).inserted_id
    
    return upload_id, upload_id_zero
    
    
def store_total_gamma(secure_url:str, spot_price:float, mongodb_upload_id:str):
    
    # Retrieve option chain from url
    option_chain_long = get_df_from_storage(secure_url=secure_url)
    option_chain_long['expiration'] = pd.to_datetime(option_chain_long['expiration'])
    
    gamma_strikes = calculate_spot_total_gamma_call_puts(
        option_chain_long=option_chain_long, 
        spot_price=spot_price)
    
    # Store it in mongodb
    mongo_doc = {
        mongodb_upload_id:gamma_strikes.reset_index().to_dict('list')
    }
    
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_GEX_STRIKES']]
        
        # Insert execution details in mongo
        upload_id = mongo_collection.insert_one(mongo_doc).inserted_id
        
    return upload_id  
        
    

def get_df_from_storage(secure_url:str) -> pd.DataFrame:
    """
    Retrieve a (compressed) dataframe from object storage

    Args:
        public_id (str): secure_url of the object stored

    Returns:
        pd.DataFrame: uncompressed dataframe from storage
    """
    query = {'raw': 'true'} 
    headers={'User-agent': 'Mozilla/5.0'}
    response = requests.get(secure_url, params=query, headers=headers, stream=True)
    
    with gzip.open(BytesIO(response.content), 'rt') as gzip_file:
        df = pd.read_csv(gzip_file, sep=',', skiprows=0, index_col=None)
        
    return df
    
def get_quote_info_from_mongo(mongodb_upload_id:str) -> pd.DataFrame:
    """
    Retrieve quote information from object storage for a given execution

    Args:
        mongodb_upload_id (str): _id of the mongodb execution

    Returns:
        pd.DataFrame: quote information for that execution
    """
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_QUOTE_INFO']]
        
        dict_info = mongo_collection.find_one(filter={'mongodb_upload_id':str(mongodb_upload_id)},)
        
        df_info = pd.DataFrame({CONFIG['CBOE_TICKER']:dict_info})
        
    return df_info

def get_upload_info_from_mongo(mongodb_upload_id:str) -> pd.DataFrame:
    """
    Retrieve quote information from object storage for a given execution

    Args:
        mongodb_upload_id (str): _id of the mongodb execution

    Returns:
        pd.DataFrame: quote information for that execution
    """
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_UPLOADS']]
        
        dict_info = mongo_collection.find_one(filter={'_id':ObjectId(mongodb_upload_id)},)
        
    return dict_info
    
    
def get_execution_id() -> pd.DataFrame:
    from data_models import IdTable
    result = session.query(IdTable)
    df_id = pd.read_sql(result.statement,session.bind)
    
    return df_id
    
    
def get_ohlc_data() -> pd.DataFrame:
    yfin_ticker = yf.Ticker(CONFIG['YFIN_TICKER'])
    
    # get historical market data
    yfin_hist = yfin_ticker.history(start='2021-01-01')
    
    return yfin_hist

def get_gex_levels_data() -> dict:
    dict_gex_levels = dict()
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_GEX_STRIKES']]
        
        cursor = mongo_collection.find({})
        for document in cursor:
            doc_keys = list(document.keys())
            dict_gex_levels[doc_keys[-1]] = document[doc_keys[-1]]
    
    return dict_gex_levels

def get_gex_profile_data() -> dict:
    dict_gex_profile = dict()
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_GEX_PROFILE']]
        
        cursor = mongo_collection.find({})
        for document in cursor:
            doc_keys = list(document.keys())
            dict_gex_profile[doc_keys[-1]] = document[doc_keys[-1]]
    
    return dict_gex_profile

def get_zero_gamma_data() -> dict:
    dict_zero_gamma = dict()
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_GEX_ZERO_GAMMA']]
        
        cursor = mongo_collection.find({})
        for document in cursor:
            doc_keys = list(document.keys())
            dict_zero_gamma[doc_keys[-1]] = document[doc_keys[-1]]
    
    return dict_zero_gamma

def update_database():
    print('Fetch new option data from source')
    # Fetch new option data and store in S3
    response = store_raw_option_chains()
    
    print('Option data stored in S3')
    
    # Add new record to SQL with execution details
    id_sql = store_execution_details_sql(response_compressed=response)
    
    print('SQL record with execution details has been created')
    
    # Get quotes info from database
    quote_info = get_quote_info_from_mongo(mongodb_upload_id=str(response['_id']))
    spot_price = quote_info.iloc[:,0]['close']
    last_trade_date = pd.to_datetime(quote_info.iloc[:,0]['lastTradeTimestamp'])
    
    print(f'Last trade date obtained: {last_trade_date}')
    
    # Calculate gamma exposure and store in database
    upload_id_profile, upload_id_zero = store_gamma_profile(
        secure_url=response['secure_url'], 
        spot_price=spot_price, 
        last_trade_date=last_trade_date, 
        mongodb_upload_id=str(response['_id']))
    
    print('Gamma profile has been stored in db')
    
    upload_id_total_gamma = store_total_gamma(
        secure_url=response['secure_url'], 
        spot_price=spot_price, 
        mongodb_upload_id=str(response['_id']))
    
    print('Total gamma has been stored')
    

if __name__ == '__main__':
    update_database()