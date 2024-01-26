from fastapi import FastAPI, status, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

import pandas as pd

import os
from dotenv import load_dotenv

# Load endpoint functions
from database import (
    get_execution_id,
    get_ohlc_data,
    get_gex_levels_data,
    get_gex_profile_data,
    get_zero_gamma_data,
    get_quote_info_from_mongo,
    store_raw_option_chains,
    store_execution_details_sql,
    store_gamma_profile,
    store_total_gamma,
)

# Get env variables
load_dotenv()
# load_dotenv(dotenv_path=os.path.join('..','.env'))


app = FastAPI(
    title="Gamma Exposure app",
    summary="Retrieves SPX option data and calculates the gamma exposure of the options",
)


# API endpoints
# ==========================
@app.get("/")
async def hello_world():
   return {"message": "hello_world"}

@app.get(
    "/execution_info",
    response_description="Get information about all executions",
    status_code=status.HTTP_200_OK,
    # response_model=StudentModel,
    # response_model_by_alias=False,
)
async def execution_info():
    df_id = get_execution_id()
    df_id['execution_timestamp'] = df_id['execution_timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    df_id['delayed_timestamp'] = df_id['delayed_timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    return df_id.to_dict('list')

@app.get(
    "/zero_gamma",
    response_description="Get gex profile data",
    status_code=status.HTTP_200_OK,
    # response_model=StudentModel,
    # response_model_by_alias=False,
)
async def zero_gamma_data():
    dict_zero_gamma = get_zero_gamma_data()
        
    return dict_zero_gamma

@app.get(
    "/gex_profile",
    response_description="Get gex profile data",
    status_code=status.HTTP_200_OK,
    # response_model=StudentModel,
    # response_model_by_alias=False,
)
async def gex_profile_data():
    dict_gex_profile = get_gex_profile_data()
        
    return dict_gex_profile

@app.get(
    "/gex_levels",
    response_description="Get gex levels data",
    status_code=status.HTTP_200_OK,
    # response_model=StudentModel,
    # response_model_by_alias=False,
)
async def gex_levels_data():
    dict_gex_levels = get_gex_levels_data()
        
    return dict_gex_levels

@app.get(
    "/ohlc_data",
    response_description="Get ohlc data",
    status_code=status.HTTP_200_OK,
    # response_model=StudentModel,
    # response_model_by_alias=False,
)
async def ohlc_data():
    ohlc = get_ohlc_data()
    ohlc = ohlc.reset_index().drop(columns={'Dividends', 'Stock Splits'})
        
    return ohlc.to_dict('list')

@app.post(
    "/update_db",
    response_description="Fetch new raw data, transform and update values in database",
    status_code=status.HTTP_201_CREATED,
    # response_model=StudentModel,
    # response_model_by_alias=False,
)
async def update_db():
    # Fetch new option data and store in S3
    response = store_raw_option_chains()
    
    # Add new record to SQL with execution details
    id_sql = store_execution_details_sql(response_compressed=response)
    
    # Get quotes info from database
    quote_info = get_quote_info_from_mongo(mongodb_upload_id=str(response['_id']))
    spot_price = quote_info.iloc[:,0]['close']
    last_trade_date = pd.to_datetime(quote_info.iloc[:,0]['lastTradeTimestamp'])
    
    # Calculate gamma exposure and store in database
    upload_id_profile, upload_id_zero = store_gamma_profile(
        secure_url=response['secure_url'], 
        spot_price=spot_price, 
        last_trade_date=last_trade_date, 
        mongodb_upload_id=str(response['_id']))
    
    upload_id_total_gamma = store_total_gamma(
        secure_url=response['secure_url'], 
        spot_price=spot_price, 
        mongodb_upload_id=str(response['_id']))
    
    return Response(status_code=status.HTTP_201_CREATED)
    
@app.post(
    "/initialize",
    response_description="Drop SQL tables and MongoDB collections and re-create them from scratch",
    status_code=status.HTTP_201_CREATED,
    # response_model=StudentModel,
    # response_model_by_alias=False,
)
async def initialize(pwd: str):
    """
    Drop all tables and collections and re-create them from scratch
    """
    
    # Check if pwd is equal to .env value
    if pwd == os.environ.get('INIT_CRED'):
        # init_result = init_db_from_scratch()
        init_result = 1
        
        if init_result.status==1:
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        
        raise HTTPException(status_code=404, detail=f"Unable to perform initialization")
    else:
        raise HTTPException(status_code=404, detail=f"Credentials not valid")
    
    