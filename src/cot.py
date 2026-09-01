import os
import sys
# cot.py sits directly in src/, so src/ is what goes on the path for the bare
# `model.*` / `controller.*` / `custom_types.*` imports below (one dirname).
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
import numpy as np
import json
import logging
import math
from typing import Optional
import cot_reports as cot
from pydantic import BaseModel
# from src.database.db import db_connect
from model.cot import CotModell
import pandas as pd
from controller.cot import COTController
from custom_types.cot import CFTCData, CotData, COT_COLUMN_MAPPING
from datetime import datetime, date
import re

class COT:
    
    def __init__(self):
        self.cot_ctrl = COTController()
        self.cot_model = CotModell()
        
    async def all_data(self):
        try:
            start_year = 2017
            end_year = 2026
            df:pd.DataFrame = pd.DataFrame()
            
            for i in range(start_year, end_year + 1):
            
                data:pd.DataFrame = pd.DataFrame(cot.cot_year(year=i,cot_report_type='disaggregated_fut'))

                df = pd.concat([df,data])
            
            df_06 = pd.DataFrame(cot.cot_all(cot_report_type='disaggregated_fut'))  
            df_06.to_csv('data/fut_2006.csv')
                    
            df.to_csv('data/cot_fut.csv') 
            return df
        except Exception as e:
            logging.error(f'Error getting all data : {e}' , exc_info=True)
            raise
                
        
        # This function merges data aggregated data from 2006 and 2017   
    async def merge_all_data(self):
        try:
            dtype_spec = {
                    'CFTC_Contract_Market_Code': str,
                    'CFTC_Contract_Market_Code_Quotes': str
                }
            pd_2006:pd.DataFrame= pd.read_csv('F_Disagg06_16.csv',dtype=dtype_spec)
            pd_recent:pd.DataFrame = pd.read_csv('f_year.csv')
        
            cot_df:pd.DataFrame = pd.concat([pd_2006,pd_recent]).convert_dtypes()
    
            cot_df.to_csv('data/cot_fut.csv')
        except Exception as e:
            logging.error(f'Error agregatting data : {e}', exc_info=True)
            raise 
        
    # Determine the asset market and standardized asset name
    async def determine_market(self, instrument:str, instrument_arr:list,data:dict):
         
        try:
            
            asset_cls = None 
            asset_name = None
            excluded_pairs = ['RUSSEL 1000 MINI INDEX FUTURE',
                              'RUSSELL 1000 VALUE INDEX MINI',
                              'EMINI RUSSELL 1000 VALUE INDEX',
                              'EMINI RUSSELL 1000 GROWTH',
                              'MICRO E-MINI RUSSELL 2000 INDX',
                              'Russell 2000 Stock Index (Mini)',
                              'RUSSELL 2000 MINI INDEX FUTURE',
                              'DOW JONES INDUSTRIAL AVERAGE', 
                              'DJIA x $5',
                              'DOW JONES INDUSTRIAL AVG- x $5', 
                              'MICRO E-MINI DJIA (x$0.5)',
                              'S&P 500 STOCK INDEX',
                              'ADJUSTED INT RATE S&P 500 TOTL',
                              'S&P 500 TOTAL RETURN INDEX',
                              'S&P 500 ANNUAL DIVIDEND INDEX',
                              'S&P 500 QUARTERLY DIVIDEND IND',
                              'MICRO E-MINI S&P 500 INDEX',
                              'E-MINI S&P REAL ESTATE INDEX',
                              'NASDAQ-100 STOCK INDEX',
                              'MICRO E-MINI NASDAQ-100 INDEX',
                              'NIKKEI STOCK AVERAGE YEN DENOM',
                              'S&P 400 MIDCAP STOCK INDEX',
                              'E-MINI S&P 400 STOCK INDEX',
                              'MSCI EAFE MINI INDEX',
                              'E-MINI MSCI EAFE',
                              'MSCI EAFE',
                              'MSCI EMERGING MKTS MINI INDEX',
                              'MSCI EMERGING MKTS MINI INDEX', 
                              'E-MINI MSCI EMERGING MARKETS', 
                              'MSCI EMERGING MKTS INDEX', 
                              'MSCI EM INDEX', 
                              'MSCI EM ASIA MINI NTR INDEX',
                              'MINI MSCI ACWI NTR INDEX',
                              '3-MO. EUROYEN TIBOR',
                              'EURO SHORT TERM RATE ',
                              'THREE-MONTH BLOOMBERG ST BANK',
                              'MICRO BITCOIN',
                              'Nano Bitcoin',
                              'NANO BITCOIN',
                              'BITCOIN-USD',
                              'MICRO ETHER',
                              'NANO ETHER',
                              'NANO ETHER PERP STYLE',
                              'LITECOIN CASH',
                              'DOGECOIN',
                              'POLKADOT',
                              'CHAINLINK',
                              'AVALANCHE',
                              '1K SHIB',
                              'STELLAR',
                              'NANO STELLAR','NANO SOLANA','SOL','MICRO SOL',
                              'CARDONA','MICRO XRP','NANO XRP', 'XRP','HEDERA',
                              'EURO FX/JAPANESE YEN'
                              ]
         
            
            if instrument in excluded_pairs:
                
                return None, None
            
            if instrument not in instrument_arr:
                return None, None
                    
        
            for index, category in data.items():
              
                asset_name = instrument

                
                # for instrument in category:
                if instrument in category:
                    
                    asset_cls = index
                    
                
                    if instrument == 'BRITISH POUND STERLING'  or instrument =='BRITISH POUND':
                        asset_name =   'BRITISH POUND'
                        
                    if instrument == 'SOUTH AFRICAN RAND' or instrument =='SO AFRICAN RAND' :  
                        asset_name = 'SOUTH AFRICAN RAND'
                        
                    if instrument == 'NEW ZEALAND DOLLAR' or instrument == 'NZ DOLLAR':
                        asset_name = 'NEW ZEALAND DOLLAR'
                        
                    if instrument == 'U.S. DOLLAR INDEX' or instrument== 'USD INDEX' :
                        asset_name = 'USD INDEX'
                        
                    if instrument == 'Russell 2000 Stock Index Future'or instrument=='Russell 2000 Stock Index'or instrument== 'E-MINI RUSSELL 2000 INDEX'or instrument=='RUSSELL E-MINI':
                        asset_name = 'E-MINI RUSSELL 2000 INDEX'
                        
                    if instrument == 'DJIA Consolidated' :
                        asset_name = 'DOW JONES INDUSTRIAL AVERAGE'
                    
                    if instrument == 'S&P 500 Consolidated':
                        asset_name = 'S&P 500 STOCK INDEX'
                        
                    if  instrument == 'E-MINI S&P 500 STOCK INDEX' or instrument == 'E-MINI S&P 500':
                        asset_name = 'E-MINI S&P 500 STOCK INDEX'
                        
                    if instrument == 'NASDAQ-100 Consolidated':
                        asset_name = 'NASDAQ-100 STOCK INDEX'
                        
                    if  instrument == 'NASDAQ-100 STOCK INDEX (MINI)' or instrument == 'NASDAQ MINI':
                        asset_name = 'NASDAQ-100 STOCK INDEX'

                    if instrument =='VIX FUTURES':
                        asset_name = 'S&P 500 VIX'
                        
                    if instrument == 'U.S. TREASURY BONDS' and instrument == 'UST BOND':
                        asset_name = '30-YEAR T-BOND'
                        
                        
                    if instrument == 'ULTRA U.S. TREASURY BONDS' and instrument == 'ULTRA UST BOND':
                        asset_name = 'ULTRA T-BOND'
                    
                    if instrument == '10-YEAR U.S. TREASURY NOTES' and instrument == 'UST 10Y NOTE':
                        asset_name = '10-YEAR T-NOTE'
                        
                    if instrument == 'ULTRA 10-YEAR U.S. T-NOTES' and instrument == 'ULTRA UST 10Y':
                        asset_name= 'ULTRA 10-YEAR T-NOTE'
                    
                    if instrument == '5-YEAR U.S. TREASURY NOTES' and instrument == 'UST 5Y NOTE':
                        asset_name = '5-YEAR T-NOTE'
                        
                    if instrument == '2-YEAR U.S. TREASURY NOTES' and instrument == 'UST 2Y NOTE':
                        asset_name = '2-YEAR T-NOTE'    
                        
                    if instrument == '30-DAY FEDERAL FUNDS' or instrument =='FED FUNDS - CHICAGO BOARD OF TRADE':
                        asset_name = '30-DAY FEDERAL FUNDS'
                        
                    if instrument == 'SOFR-3M - CHICAGO MERCANTILE EXCHANGE' or instrument == '3-MONTH SOFR - CHICAGO MERCANTILE EXCHANGE':
                        asset_name = '3-MONTH SOFR'
                        
                    return   asset_cls,asset_name
            
                        
        except Exception as e: 
            logging.error(f'Error determine market type : {e}', exc_info=True)
            raise
        
        
    async def process_all_data(self):
        try:
            with open('data/instr.json', 'r') as f:
                data = json.load(f)
            # Concatenate all array into one array
            merged = []
            for category, items in data.items():
                merged.extend(items)
            
            load_df:pd.DataFrame= pd.read_csv('data/cot_all.csv')
            load_df.rename(columns=COT_COLUMN_MAPPING, inplace=True)
            

            # Check for any columns that weren't renamed
            missing_columns = set(load_df.columns) - set(COT_COLUMN_MAPPING.values())
            if missing_columns:
                logging.info(f"Warning: These columns weren't renamed: {missing_columns}")
                        
            # Rename the columns for ease of manipulation
            instrument_list:list =[]
            
            
            rows = load_df.to_dict('records')
            for row in rows:
                # Determine Market type
                instrument = row["market_and_exchange_names"].split(" - ")
                
                asset_cls,asset_name = await self.determine_market(str(instrument[0]),merged,data)
                
                if asset_cls != None and  asset_name != None :
                                    # Perform an action  
                    new_row = await self.clean_row_data(row) 
                    new_row["market_and_exchange_names"] = asset_name
                    new_row["market"] = asset_cls
                    
                    if new_row == {} or new_row == None:
                        logging.info("New Row was empty")
                        return
                    
                    new_cot = CotData(**new_row).model_dump()
                    
                    instrument_list.append(new_cot)
                  
            if len(instrument_list) > 0 :
                data = await self.cot_model.insert_tff_report(data)
                
        except Exception as e:
            logging.error(f"Error processing all data: {e}", exc_info=True)
            raise
        
    async def clean_row_data(self, row_dict: dict) -> dict:
        cleaned = {}
        
        for key, value in row_dict.items():
            # Handle empty/missing values first
            if pd.isna(value) or value == '.':
                cleaned[key] = None
                continue
            
            # Handle date field
            if key == 'report_date_as_yyyy_mm_dd' and isinstance(value, str):
                try:
                    if re.match(r'^\d{4}-\d{2}-\d{2}$', value):
                        tmp_date = datetime.strptime(value, '%Y-%m-%d')
                    else:
                        # Try to parse date with time
                        tmp_date = datetime.strptime(value, '%m/%d/%Y %I:%M:%S %p')
                    cleaned[key] = tmp_date
                    continue  # Skip remaining processing for this field
                except ValueError:
                    # If date parsing fails, keep original value
                    cleaned[key] = value
                    continue
            
            # Handle 'change_in_' fields (convert to float)
            if 'change_in_' in key and isinstance(value, (str, int, float)):
                try:
                    cleaned[key] = float(value)
                    continue
                except (ValueError, TypeError):
                    cleaned[key] = None
                    continue
            
            # Handle 'traders_' fields (convert string to float, float to int)
            if key.startswith('traders_'):
                if isinstance(value, int):
                    cleaned[key] = float(value)  # Convert float to int for trader counts
                elif isinstance(value, str):
                    try:
                        cleaned[key] = float(value)  # Convert string to float
                    except ValueError:
                        cleaned[key] = None  # If conversion fails, set to None
                else:
                    cleaned[key] = value
                continue
            
            # Handle other string fields (strip whitespace)
            if isinstance(value, str):
                cleaned[key] = value.strip()
            else:
                cleaned[key] = value
        
        return cleaned
    
    async def update_cot(self):
        try:
            last_entry = await  self.cot_model.get_last_entry()
           
            if not last_entry:
                return 
            
            # 2. Create lookup map
            instrument_map = {e.market_and_exchange_names: e for e in last_entry}
            
            # 3. Load instruments
            with open('data/instr.json', 'r') as f:
                data = json.load(f)
                
            merged = [item for items in data.values() for item in items]
            
            # 4. Fetch current year data
            year = datetime.now().year
            df:pd.DataFrame = cot.cot_year(year=year, cot_report_type='traders_in_financial_futures_fut')
            df.rename(columns=COT_COLUMN_MAPPING, inplace=True)
            
            
            
            # Check for any columns that weren't renamed
            missing_columns = set(df.columns) - set(COT_COLUMN_MAPPING.values())
           
            
            if df.empty:
                return
        
        # 5. Process data in batches
            updated_cot_list = []  
            rows = df.to_dict('records')
            for row in rows:
                # Determine Market type
                instrument = row["market_and_exchange_names"].split(" - ")
                
                asset_cls,asset_name = await self.determine_market(str(instrument[0]),merged,data)
                    
                if not asset_cls or not asset_name:
                    continue
                
                new_row = await self.clean_row_data(row) 
                
            
                new_row["market_and_exchange_names"] = asset_name
                new_row["market"] = asset_cls
                
                if asset_name in instrument_map:
                    
                    new_date =  (new_row['report_date_as_yyyy_mm_dd'])
                    old_date = (instrument_map[asset_name].report_date_as_yyyy_mm_dd)
                    
                    
                    if new_date > old_date:
                        updated_cot_list.append(CotData(**new_row))
          
            if updated_cot_list:
            # Remove duplicates
                unique_data = {
                    (item.market_and_exchange_names, item.report_date_as_yyyy_mm_dd): item 
                    for item in updated_cot_list
                }.values()
                
                # Convert to dicts for database
                cot_models = list(unique_data)
                
                await self.cot_model.update_ttf_report(cot_models)
                await self.cot_ctrl.insert_cot_redis(cot_models)

                logging.info("Sucessfully updated Cot Data", exc_info=True)
                
        except Exception as e:
            logging.error(f'Error updating cot data : {e}', exc_info=True)
            raise
            
# test = COTNew()
# val = asyncio.run(test.update_cot())
# print(len(val))