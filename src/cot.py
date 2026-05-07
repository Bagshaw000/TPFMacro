import os
import sys

import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import asyncio
import json
import logging
import math
from typing import Optional
import cot_reports as cot
from pydantic import BaseModel
from src.database.db import db_connect
from model import CotModel
import pandas as pd
from controller.cot import COTController
from custom_types.cot import CFTCData
from datetime import datetime, date
import re

class COT:
    
    def __init__(self):
        self.db = db_connect()
        self.cot_ctrl = COTController()
        self.cot_model = CotModel()
        
    async def get_cot(self):
        try:
            data_exist = await self.cot_model.get_last_report()
            
            if data_exist.count:
                pass
            
            if not data_exist.count:
             
                
                cot_data = await self.all_data()
                
              
                return cot_data
            
        except Exception as e:
            logging.error(f'Error getting cot report : {e}')
    
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
            
    async def process_all_data(self):
        try:
            
         
            with open('data/instr.json', 'r') as f:
                data = json.load(f)
            # Concatenate all array into one array
            merged = []
            for category, items in data.items():
                merged.extend(items)
                
            load_df:pd.DataFrame= pd.read_csv('data/cot_all.csv')
           
            
            instrument_list:list =[]
           
            for index, row in load_df.iterrows():
                # Determin market type
                instrument = row['Market_and_Exchange_Names'].split(' - ')
                
             
                asset_cls,asset_name = await self.determine_market(instrument[0],merged,data)
                # Map to a dataframe to be inserted into the databasae
               
                
                if asset_cls != None and  asset_name != None :
                    # Perform an action  
                    new_row = self.clean_row_data(row.to_dict()) 
                    date = new_row.get('Report_Date_as_YYYY-MM-DD')
               
                    # Use regex to format datetime 'Report_Date_as_YYYY_MM_DD'
                    if re.match(r'^\d{4}-\d{2}-\d{2}$', date):
                        
                        new_row['Report_Date_as_YYYY_MM_DD'] = datetime.strptime(date, '%Y-%m-%d').isoformat()
                    else:
                        new_row['Report_Date_as_YYYY_MM_DD'] = datetime.strptime(datetime.strptime(date, '%m/%d/%Y %I:%M:%S %p').strftime('%Y-%m-%d'), '%Y-%m-%d').isoformat()
                    
                    # Drop na values and .
                    new_row = {k: (None if pd.isna(v) else v) for k, v in new_row.items()}
                    new_row = {k: (None if v=='.' else v) for k, v in new_row.items()}
                  
                    # Re assign variable names
                    new_row['Market_and_Exchange_Names']= asset_name
                    new_row['Market']= asset_cls
                    
                    # Json serialize the data
                    new_cftc = CFTCData(**new_row).model_dump()
                    
                    # Append the to array to inserted in the database
                    
                    instrument_list.append(new_cftc)
                    
            len_instr = len(instrument_list)
            
            # Insert into database
            if len_instr > 0:
                loop_len= math.ceil( len_instr/1000)
                
                for i in range(loop_len):
                 
                    if i == 0 :
                        data = instrument_list[:1000]
                      
                        await self.cot_model.insert_tff_report(data)
                    if i == len_instr:
                        start_index =(i*1000) + 1
                        data = instrument_list[start_index : ]
                       
                        await self.cot_model.insert_tff_report(data)
                    else:
                        start_index =(i*1000) + 1
                        end_index = start_index + 999
                        data = instrument_list[start_index : end_index]
                        
                        await self.cot_model.insert_tff_report(data)
              
              
                return True
                
            
            return False
            
            
        except Exception as e:
            logging.error(f'Error processing all data : {e}', exc_info=True)
            
            
    def clean_row_data(self,row_dict:dict) ->dict:
        cleaned = {}
        for key, value in row_dict.items():
            if isinstance(value, str) and key != 'Report_Date_as_YYYY-MM-DD':
                cleaned[key] = value.strip()  # Removes leading/trailing spaces
            else:
                cleaned[key] = value
        return cleaned
            
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
    
    # This function classifies instrument into assets clase and format asset name 
    async def extract_instruments(self):
        try:
            df:pd.DataFrame = pd.read_json('instrument_group.json')
            
            data= {
                'Currency' :[],
                'Indices':[],
                'Financial':[],
                'Crypto':[]
            }

            
            for index, row in df.items():
                  
                if isinstance(row,pd.Series) :
                    if index =='Currency':
                        for sub_cat, items in row.items():
                     
                            if isinstance(items, list):
                                for item in items:
                                    curr = item.split(' - ')
            
                                    data['Currency'].append(curr[0])
                                   
                       
                    if index == 'Indices':
                        for sub_cat, items in row.items():
                       
                            if isinstance(items, list):
                                for item in items:
                                    curr = item.split(' - ')
            
                                    data['Indices'].append(curr[0])
                    if index =='Financial':
                        for sub_cat, items in row.items():
                      
                            if isinstance(items, list):
                                for item in items:
                                    curr = item.split(' - ')
            
                                    data['Financial'].append(curr[0])
                    if index =='Crypto':
                        for sub_cat, items in row.items():
                        
                            if isinstance(items, list):
                                for item in items:
                                    curr = item.split(' - ')
            
                                    data['Crypto'].append(curr[0])
                    
                    
            pd.Series(data).to_json('data/instr.json')
                                
                       
        except Exception as e:
            logging.error(f'Error extracting instruments', exc_info=True)
    
    
          
    async def instruments(self):
        instrument_df:pd.DataFrame = pd.read_csv('data/cot_fut.csv')
        data = instrument_df['Market_and_Exchange_Names'].unique()
        pd.Series(data).to_json('data/instrument_fut.json', orient='records')
        
    # This function updates cot data
    async def update_cot(self):
        try:
            # Get all the last entrries for all 
            last_entry = await self.cot_model.get_last_entry()
            # print(last_entry)
            # return
            instrument_map = dict()
            updated_cot_list = list()
            
            # Create a hashmap of all data with asset name
            for element in last_entry.data:
                instrument_map[element['Market_and_Exchange_Names']] = element
                
           
            # Get data list of all instument
            with open('data/instr.json', 'r') as f:
                data = json.load(f)
                
            # Merge the all instrument into one array
            merged = []
            for category, items in data.items():
                merged.extend(items)
                

            # Get the current year
            year = datetime.now().year
            
            # Get the cot report for the current year
            df:pd.DataFrame = cot.cot_year(year = year, cot_report_type = 'traders_in_financial_futures_fut')

            
            # loop through the dataframe
            for index, row  in df.iterrows():
                # Determine market class and format the market name
                instrument = row['Market_and_Exchange_Names'].split(' - ')
                
                # Filters assets we track
                asset_cls,asset_name = await self.determine_market(instrument[0],merged,data)
                
                if asset_cls == None or asset_name == None:
                    continue
                
                # Clean data from each row 
                cleaned_row = self.clean_row_data(row.to_dict())
                
                date = cleaned_row.get('Report_Date_as_YYYY-MM-DD')
               
                # Use regex to format datetime 'Report_Date_as_YYYY_MM_DD'
                if re.match(r'^\d{4}-\d{2}-\d{2}$', date):
                    
                    cleaned_row['Report_Date_as_YYYY_MM_DD'] = datetime.strptime(date, '%Y-%m-%d').isoformat()
                else:
                    cleaned_row['Report_Date_as_YYYY_MM_DD'] = datetime.strptime(datetime.strptime(date, '%m/%d/%Y %I:%M:%S %p').strftime('%Y-%m-%d'), '%Y-%m-%d').isoformat()
                
                # Drop na values and '.'
                cleaned_row = {k: (None if pd.isna(v) else v) for k, v in cleaned_row.items()}
                cleaned_row = {k: (None if v=='.' else v) for k, v in cleaned_row.items()}
                
                cleaned_row['Market_and_Exchange_Names']= asset_name
                cleaned_row['Market']= asset_cls
                    
                new_cftc = CFTCData(**cleaned_row).model_dump()
            
                # Check if the is the cot report is database
                if asset_name in instrument_map.keys():
                    
                    element_date = datetime.fromisoformat(new_cftc["Report_Date_as_YYYY_MM_DD"])
                    last_element_date = datetime.fromisoformat(instrument_map[asset_name]["Report_Date_as_YYYY_MM_DD"])

                    # Get latest cot data 
                    if element_date > last_element_date:
                        updated_cot_list.append(new_cftc)
            
            #  Update database 
            if len(updated_cot_list) != 0:
                dup_df = pd.DataFrame(updated_cot_list)
                df_unique = dup_df.drop_duplicates(subset=['Market_and_Exchange_Names', 'Report_Date_as_YYYY_MM_DD'],keep="last").replace({np.nan: None, np.inf: None, -np.inf: None})
                unique_dicts = df_unique.to_dict('records')
                
                cftc_models = [CFTCData(**dict_item).model_dump() for dict_item in unique_dicts]
         
                
                await self.cot_model.update_ttf_report(cftc_models)
                await self.cot_ctrl.insert_cot_redis(cftc_models)
                print("Done")
             
        except Exception as e:
            logging.error(f'Error updating cot data : {e}', exc_info=True)
            
    

    
        
# 'Market_and_Exchange_Names', 'As_of_Date_In_Form_YYMMDD',
    #    'Report_Date_as_YYYY-MM-DD', 'CFTC_Contract_Market_Code',
    #    'CFTC_Market_Code', 'CFTC_Region_Code', 'CFTC_Commodity_Code',
    #    'Open_Interest_All', 'Dealer_Positions_Long_All',
    #    'Dealer_Positions_Short_All', 'Dealer_Positions_Spread_All',
    #    'Asset_Mgr_Positions_Long_All', 'Asset_Mgr_Positions_Short_All',
    #    'Asset_Mgr_Positions_Spread_All', 'Lev_Money_Positions_Long_All',
    #    'Lev_Money_Positions_Short_All', 'Lev_Money_Positions_Spread_All',
    #    'Other_Rept_Positions_Long_All', 'Other_Rept_Positions_Short_All',
    #    'Other_Rept_Positions_Spread_All', 'Tot_Rept_Positions_Long_All',
    #    'Tot_Rept_Positions_Short_All', 'NonRept_Positions_Long_All',
    #    'NonRept_Positions_Short_All', 'Change_in_Open_Interest_All',
    #    'Change_in_Dealer_Long_All', 'Change_in_Dealer_Short_All',
    #    'Change_in_Dealer_Spread_All', 'Change_in_Asset_Mgr_Long_All',
    #    'Change_in_Asset_Mgr_Short_All', 'Change_in_Asset_Mgr_Spread_All',
    #    'Change_in_Lev_Money_Long_All', 'Change_in_Lev_Money_Short_All',
    #    'Change_in_Lev_Money_Spread_All', 'Change_in_Other_Rept_Long_All',
    #    'Change_in_Other_Rept_Short_All', 'Change_in_Other_Rept_Spread_All',
    #    'Change_in_Tot_Rept_Long_All', 'Change_in_Tot_Rept_Short_All',
    #    'Change_in_NonRept_Long_All', 'Change_in_NonRept_Short_All',
    #    'Pct_of_Open_Interest_All', 'Pct_of_OI_Dealer_Long_All',
    #    'Pct_of_OI_Dealer_Short_All', 'Pct_of_OI_Dealer_Spread_All',
    #    'Pct_of_OI_Asset_Mgr_Long_All', 'Pct_of_OI_Asset_Mgr_Short_All',
    #    'Pct_of_OI_Asset_Mgr_Spread_All', 'Pct_of_OI_Lev_Money_Long_All',
    #    'Pct_of_OI_Lev_Money_Short_All', 'Pct_of_OI_Lev_Money_Spread_All',
    #    'Pct_of_OI_Other_Rept_Long_All', 'Pct_of_OI_Other_Rept_Short_All',
    #    'Pct_of_OI_Other_Rept_Spread_All', 'Pct_of_OI_Tot_Rept_Long_All',
    #    'Pct_of_OI_Tot_Rept_Short_All', 'Pct_of_OI_NonRept_Long_All',
    #    'Pct_of_OI_NonRept_Short_All', 'Traders_Tot_All',
    #    'Traders_Dealer_Long_All', 'Traders_Dealer_Short_All',
    #    'Traders_Dealer_Spread_All', 'Traders_Asset_Mgr_Long_All',
    #    'Traders_Asset_Mgr_Short_All', 'Traders_Asset_Mgr_Spread_All',
    #    'Traders_Lev_Money_Long_All', 'Traders_Lev_Money_Short_All',
    #    'Traders_Lev_Money_Spread_All', 'Traders_Other_Rept_Long_All',
    #    'Traders_Other_Rept_Short_All', 'Traders_Other_Rept_Spread_All',
    #    'Traders_Tot_Rept_Long_All', 'Traders_Tot_Rept_Short_All',
    #    'Conc_Gross_LE_4_TDR_Long_All', 'Conc_Gross_LE_4_TDR_Short_All',
    #    'Conc_Gross_LE_8_TDR_Long_All', 'Conc_Gross_LE_8_TDR_Short_All',
    #    'Conc_Net_LE_4_TDR_Long_All', 'Conc_Net_LE_4_TDR_Short_All',
    #    'Conc_Net_LE_8_TDR_Long_All', 'Conc_Net_LE_8_TDR_Short_All',
    #    'Contract_Units', 'CFTC_Contract_Market_Code_Quotes',
    #    'CFTC_Market_Code_Quotes', 'CFTC_Commodity_Code_Quotes',
    #    'CFTC_SubGroup_Code', 'FutOnly_or_Combined'
    
    # Market_and_Exchange_Names,As_of_Date_In_Form_YYMMDD,
    # Report_Date_as_YYYY-MM-DD,CFTC_Contract_Market_Code,
    # CFTC_Market_Code,CFTC_Region_Code,
    # CFTC_Commodity_Code,Open_Interest_All,
    # Prod_Merc_Positions_Long_All,Prod_Merc_Positions_Short_All,
    # Swap_Positions_Long_All,Swap__Positions_Short_All,
    # Swap__Positions_Spread_All,M_Money_Positions_Long_All,
    # M_Money_Positions_Short_All,M_Money_Positions_Spread_All,
    # Other_Rept_Positions_Long_All,Other_Rept_Positions_Short_All,
    # Other_Rept_Positions_Spread_All,Tot_Rept_Positions_Long_All,
    # Tot_Rept_Positions_Short_All,NonRept_Positions_Long_All,
    # NonRept_Positions_Short_All,Open_Interest_Old,Prod_Merc_Positions_Long_Old,
    # Prod_Merc_Positions_Short_Old,Swap_Positions_Long_Old,
    # Swap__Positions_Short_Old,Swap__Positions_Spread_Old,M_Money_Positions_Long_Old,
    # M_Money_Positions_Short_Old,M_Money_Positions_Spread_Old,Other_Rept_Positions_Long_Old,
    # Other_Rept_Positions_Short_Old,Other_Rept_Positions_Spread_Old,Tot_Rept_Positions_Long_Old,
    # Tot_Rept_Positions_Short_Old,NonRept_Positions_Long_Old,NonRept_Positions_Short_Old,
    # Open_Interest_Other,Prod_Merc_Positions_Long_Other,Prod_Merc_Positions_Short_Other,
    # Swap_Positions_Long_Other,Swap__Positions_Short_Other,Swap__Positions_Spread_Other,
    # M_Money_Positions_Long_Other,M_Money_Positions_Short_Other,
    # M_Money_Positions_Spread_Other,Other_Rept_Positions_Long_Other,
    # Other_Rept_Positions_Short_Other,Other_Rept_Positions_Spread_Other,
    # Tot_Rept_Positions_Long_Other,Tot_Rept_Positions_Short_Other,
    # NonRept_Positions_Long_Other,NonRept_Positions_Short_Other,
    # Change_in_Open_Interest_All,
    # Change_in_Prod_Merc_Long_All,Change_in_Prod_Merc_Short_All,
    # Change_in_Swap_Long_All,Change_in_Swap_Short_All,
    # Change_in_Swap_Spread_All,
    # Change_in_M_Money_Long_All,
    # Change_in_M_Money_Short_All,
    # Change_in_M_Money_Spread_All,Change_in_Other_Rept_Long_All,
    # Change_in_Other_Rept_Short_All,Change_in_Other_Rept_Spread_All,
    # Change_in_Tot_Rept_Long_All,Change_in_Tot_Rept_Short_All,
    # Change_in_NonRept_Long_All,Change_in_NonRept_Short_All,
    # Pct_of_Open_Interest_All,Pct_of_OI_Prod_Merc_Long_All,
    # Pct_of_OI_Prod_Merc_Short_All,Pct_of_OI_Swap_Long_All,
    # Pct_of_OI_Swap_Short_All,Pct_of_OI_Swap_Spread_All,
    # Pct_of_OI_M_Money_Long_All,Pct_of_OI_M_Money_Short_All,
    # Pct_of_OI_M_Money_Spread_All,Pct_of_OI_Other_Rept_Long_All,
    # Pct_of_OI_Other_Rept_Short_All,Pct_of_OI_Other_Rept_Spread_All,
    # Pct_of_OI_Tot_Rept_Long_All,Pct_of_OI_Tot_Rept_Short_All,
    # Pct_of_OI_NonRept_Long_All,Pct_of_OI_NonRept_Short_All,
    # Pct_of_Open_Interest_Old,Pct_of_OI_Prod_Merc_Long_Old,
    # Pct_of_OI_Prod_Merc_Short_Old,Pct_of_OI_Swap_Long_Old,
    # Pct_of_OI_Swap_Short_Old,Pct_of_OI_Swap_Spread_Old,
    # Pct_of_OI_M_Money_Long_Old,Pct_of_OI_M_Money_Short_Old,
    # Pct_of_OI_M_Money_Spread_Old,
    # Pct_of_OI_Other_Rept_Long_Old,Pct_of_OI_Other_Rept_Short_Old,
    # Pct_of_OI_Other_Rept_Spread_Old,
    # Pct_of_OI_Tot_Rept_Long_Old,Pct_of_OI_Tot_Rept_Short_Old,
    # Pct_of_OI_NonRept_Long_Old,Pct_of_OI_NonRept_Short_Old,
    # Pct_of_Open_Interest_Other,Pct_of_OI_Prod_Merc_Long_Other,
    # Pct_of_OI_Prod_Merc_Short_Other,Pct_of_OI_Swap_Long_Other,
    # Pct_of_OI_Swap_Short_Other,Pct_of_OI_Swap_Spread_Other,
    # Pct_of_OI_M_Money_Long_Other,Pct_of_OI_M_Money_Short_Other,
    # Pct_of_OI_M_Money_Spread_Other,
    # Pct_of_OI_Other_Rept_Long_Other,Pct_of_OI_Other_Rept_Short_Other,
    # Pct_of_OI_Other_Rept_Spread_Other,Pct_of_OI_Tot_Rept_Long_Other,
    # Pct_of_OI_Tot_Rept_Short_Other,
    # Pct_of_OI_NonRept_Long_Other,Pct_of_OI_NonRept_Short_Other,
    # Traders_Tot_All,Traders_Prod_Merc_Long_All,Traders_Prod_Merc_Short_All,Traders_Swap_Long_All,Traders_Swap_Short_All,
    # Traders_Swap_Spread_All,Traders_M_Money_Long_All,Traders_M_Money_Short_All,
    # Traders_M_Money_Spread_All,Traders_Other_Rept_Long_All,Traders_Other_Rept_Short_All,Traders_Other_Rept_Spread_All,
    # Traders_Tot_Rept_Long_All,Traders_Tot_Rept_Short_All,Traders_Tot_Old,
    # Traders_Prod_Merc_Long_Old,Traders_Prod_Merc_Short_Old,Traders_Swap_Long_Old,
    # Traders_Swap_Short_Old,Traders_Swap_Spread_Old,
    # Traders_M_Money_Long_Old,Traders_M_Money_Short_Old,Traders_M_Money_Spread_Old,
    # Traders_Other_Rept_Long_Old,Traders_Other_Rept_Short_Old,
    # Traders_Other_Rept_Spread_Old,Traders_Tot_Rept_Long_Old,
    # Traders_Tot_Rept_Short_Old,Traders_Tot_Other,Traders_Prod_Merc_Long_Other,
    # Traders_Prod_Merc_Short_Other,Traders_Swap_Long_Other,Traders_Swap_Short_Other,
    # Traders_Swap_Spread_Other,Traders_M_Money_Long_Other,
    # Traders_M_Money_Short_Other,Traders_M_Money_Spread_Other,
    # Traders_Other_Rept_Long_Other,Traders_Other_Rept_Short_Other,
    # Traders_Other_Rept_Spread_Other,Traders_Tot_Rept_Long_Other,Traders_Tot_Rept_Short_Other,
    # Conc_Gross_LE_4_TDR_Long_All,Conc_Gross_LE_4_TDR_Short_All,Conc_Gross_LE_8_TDR_Long_All,
    # Conc_Gross_LE_8_TDR_Short_All,Conc_Net_LE_4_TDR_Long_All,
    # Conc_Net_LE_4_TDR_Short_All,Conc_Net_LE_8_TDR_Long_All,Conc_Net_LE_8_TDR_Short_All,
    # Conc_Gross_LE_4_TDR_Long_Old,Conc_Gross_LE_4_TDR_Short_Old,Conc_Gross_LE_8_TDR_Long_Old,
    # Conc_Gross_LE_8_TDR_Short_Old,Conc_Net_LE_4_TDR_Long_Old,Conc_Net_LE_4_TDR_Short_Old,
    # Conc_Net_LE_8_TDR_Long_Old,Conc_Net_LE_8_TDR_Short_Old,Conc_Gross_LE_4_TDR_Long_Other,
    # Conc_Gross_LE_4_TDR_Short_Other,Conc_Gross_LE_8_TDR_Long_Other,Conc_Gross_LE_8_TDR_Short_Other,
    # Conc_Net_LE_4_TDR_Long_Other,Conc_Net_LE_4_TDR_Short_Other,Conc_Net_LE_8_TDR_Long_Other,Conc_Net_LE_8_TDR_Short_Other,
    # Contract_Units,CFTC_Contract_Market_Code_Quotes,CFTC_Market_Code_Quotes,CFTC_Commodity_Code_Quotes,CFTC_SubGroup_Code,FutOnly_or_Combined
    
# class CFTCData(BaseModel):
#     '''CFTC Commitments of Traders Report Data Model matching database schema'''
    
#     # Identification fields
#     Market_and_Exchange_Names: Optional[str] = None
#     As_of_Date_In_Form_YYMMDD: Optional[int] = None  # bigint
#     Report_Date_as_YYYY_MM_DD: Optional[str] = None  # text
#     CFTC_Contract_Market_Code: Optional[str] = None
#     CFTC_Market_Code: Optional[str] = None
#     CFTC_Region_Code: Optional[int] = None  # bigint
#     CFTC_Commodity_Code: Optional[int] = None  # bigint
    
#     # Open Interest
#     Open_Interest_All: Optional[int] = None  # bigint
    
#     # Dealer Positions
#     Dealer_Positions_Long_All: Optional[int] = None  # bigint
#     Dealer_Positions_Short_All: Optional[int] = None  # bigint
#     Dealer_Positions_Spread_All: Optional[int] = None  # bigint
    
#     # Asset Manager Positions
#     Asset_Mgr_Positions_Long_All: Optional[int] = None  # bigint
#     Asset_Mgr_Positions_Short_All: Optional[int] = None  # bigint
#     Asset_Mgr_Positions_Spread_All: Optional[int] = None  # bigint
    
#     # Leveraged Money Positions
#     Lev_Money_Positions_Long_All: Optional[int] = None  # bigint
#     Lev_Money_Positions_Short_All: Optional[int] = None  # bigint
#     Lev_Money_Positions_Spread_All: Optional[int] = None  # bigint
    
#     # Other Reportable Positions
#     Other_Rept_Positions_Long_All: Optional[int] = None  # bigint
#     Other_Rept_Positions_Short_All: Optional[int] = None  # bigint
#     Other_Rept_Positions_Spread_All: Optional[int] = None  # bigint
    
#     # Total Reportable Positions
#     Tot_Rept_Positions_Long_All: Optional[int] = None  # bigint
#     Tot_Rept_Positions_Short_All: Optional[int] = None  # bigint
    
#     # Non-Reportable Positions
#     NonRept_Positions_Long_All: Optional[int] = None  # bigint
#     NonRept_Positions_Short_All: Optional[int] = None  # bigint
    
#     # Changes (all text in database)
#     Change_in_Open_Interest_All: Optional[str] = None
#     Change_in_Dealer_Long_All: Optional[str] = None
#     Change_in_Dealer_Short_All: Optional[str] = None
#     Change_in_Dealer_Spread_All: Optional[str] = None
#     Change_in_Asset_Mgr_Long_All: Optional[str] = None
#     Change_in_Asset_Mgr_Short_All: Optional[str] = None
#     Change_in_Asset_Mgr_Spread_All: Optional[str] = None
#     Change_in_Lev_Money_Long_All: Optional[str] = None
#     Change_in_Lev_Money_Short_All: Optional[str] = None
#     Change_in_Lev_Money_Spread_All: Optional[str] = None
#     Change_in_Other_Rept_Long_All: Optional[str] = None
#     Change_in_Other_Rept_Short_All: Optional[str] = None
#     Change_in_Other_Rept_Spread_All: Optional[str] = None
#     Change_in_Tot_Rept_Long_All: Optional[str] = None
#     Change_in_Tot_Rept_Short_All: Optional[str] = None
#     Change_in_NonRept_Long_All: Optional[str] = None
#     Change_in_NonRept_Short_All: Optional[str] = None
    
#     # Percent of Open Interest
#     Pct_of_Open_Interest_All: Optional[int] = None  # bigint
#     Pct_of_OI_Dealer_Long_All: Optional[float] = None  # double precision
#     Pct_of_OI_Dealer_Short_All: Optional[float] = None
#     Pct_of_OI_Dealer_Spread_All: Optional[float] = None
#     Pct_of_OI_Asset_Mgr_Long_All: Optional[float] = None
#     Pct_of_OI_Asset_Mgr_Short_All: Optional[float] = None
#     Pct_of_OI_Asset_Mgr_Spread_All: Optional[float] = None
#     Pct_of_OI_Lev_Money_Long_All: Optional[float] = None
#     Pct_of_OI_Lev_Money_Short_All: Optional[float] = None
#     Pct_of_OI_Lev_Money_Spread_All: Optional[float] = None
#     Pct_of_OI_Other_Rept_Long_All: Optional[float] = None
#     Pct_of_OI_Other_Rept_Short_All: Optional[float] = None
#     Pct_of_OI_Other_Rept_Spread_All: Optional[float] = None
#     Pct_of_OI_Tot_Rept_Long_All: Optional[float] = None
#     Pct_of_OI_Tot_Rept_Short_All: Optional[float] = None
#     Pct_of_OI_NonRept_Long_All: Optional[float] = None
#     Pct_of_OI_NonRept_Short_All: Optional[float] = None
    
#     # Traders Count
#     Traders_Tot_All: Optional[int] = None  # bigint
#     Traders_Dealer_Long_All: Optional[str] = None  # text
#     Traders_Dealer_Short_All: Optional[str] = None
#     Traders_Dealer_Spread_All: Optional[str] = None
#     Traders_Asset_Mgr_Long_All: Optional[str] = None
#     Traders_Asset_Mgr_Short_All: Optional[str] = None
#     Traders_Asset_Mgr_Spread_All: Optional[str] = None
#     Traders_Lev_Money_Long_All: Optional[str] = None
#     Traders_Lev_Money_Short_All: Optional[str] = None
#     Traders_Lev_Money_Spread_All: Optional[str] = None
#     Traders_Other_Rept_Long_All: Optional[str] = None
#     Traders_Other_Rept_Short_All: Optional[str] = None
#     Traders_Other_Rept_Spread_All: Optional[str] = None
#     Traders_Tot_Rept_Long_All: Optional[int] = None  # bigint
#     Traders_Tot_Rept_Short_All: Optional[int] = None  # bigint
    
#     # Concentration (Gross)
#     Conc_Gross_LE_4_TDR_Long_All: Optional[float] = None  # double precision
#     Conc_Gross_LE_4_TDR_Short_All: Optional[float] = None
#     Conc_Gross_LE_8_TDR_Long_All: Optional[float] = None
#     Conc_Gross_LE_8_TDR_Short_All: Optional[float] = None
    
#     # Concentration (Net)
#     Conc_Net_LE_4_TDR_Long_All: Optional[float] = None  # double precision
#     Conc_Net_LE_4_TDR_Short_All: Optional[float] = None
#     Conc_Net_LE_8_TDR_Long_All: Optional[float] = None
#     Conc_Net_LE_8_TDR_Short_All: Optional[float] = None
    
#     # Additional fields
#     Contract_Units: Optional[str] = None
#     CFTC_Contract_Market_Code_Quotes: Optional[str] = None
#     CFTC_Market_Code_Quotes: Optional[str] = None
#     CFTC_Commodity_Code_Quotes: Optional[int] = None  # bigint
#     CFTC_SubGroup_Code: Optional[str] = None
#     FutOnly_or_Combined: Optional[str] = None
    
#     # Primary key and additional required fields
#     # id: int  # bigint not null
#     Market: str  # text not null