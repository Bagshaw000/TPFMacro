import asyncio
from datetime import datetime, timedelta
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import get_doppler_env
from database.redis_ import RedisConnection
import requests
from typing import List
import numpy as np
import requests
import http.client, urllib.parse
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

country=["US","CA","JP","DE","UK","AU","IN","CN","KR","BR","FR"]

class NewsSentimentController:
    
    def __init__(self):
        
        self.secrets = get_doppler_env()
        self.redis = RedisConnection()
        
    async def get_news_article(self,country:str)->dict | None:
        try:
            if country == None or country == "": 
                return
            today = datetime.now() - timedelta(hours=24)
            published_on = today.strftime("%Y-%m-%dT%H:%M")
            
            
            conn = http.client.HTTPSConnection('api.marketaux.com')
            
            params = urllib.parse.urlencode({

                'limit': 3,
                "published_after": published_on,
                "search": "economy",
                "language": "en",
                "api_token": self.secrets.news_token,
                "countries": f"{country.lower()}"
                })

            
            conn.request('GET', '/v1/news/all?{}'.format(params))

            res = conn.getresponse()
            
            if res.status != 200:
                return None
            
            data = res.read()

            res_json= json.loads(data.decode('utf-8'))
            
            
            if res_json['data'] == [] :
                return None      
            
            
            res_dict = {
                'data': res_json['data'],
                'country': country
            }
            
            return res_dict
       
        except Exception as e:
            logging.error(f"Error getting new article: {e}", exc_info=True)
            raise
    
    
    async def news_sentiment(self, country:str):
        try:
            analyzer = SentimentIntensityAnalyzer()
            news = await self.get_news_article(country)
            
            
            if news == None:
                return
            
            if news["data"] == []:
                return
            
            
            pol = 0
            for element in news["data"]:
                pol_val = analyzer.polarity_scores(element['description'])
                pol = pol + pol_val["compound"]
                
            pol_score= pol/len(news["data"])
            
            senti = await self.store_sentiment(country, pol_score)
            
            
            return senti
            
            # Store sentiment
        except Exception as e:
            logging.error(f"Error calculating news sentiment: {e}", exc_info=True)
            raise
        
        
    async def store_sentiment(self, country:str, sentiment_score:float):
        try:
            redis = await self.redis.get_async_redis()
            
            key = f"sentiment_news:{country.upper()}"
            senti = await redis.set(key, sentiment_score, ex=14400)
            return senti
        except Exception as e:
            logging.error(f"Error storing sentiment value for country{country}: {e}",exc_info=True)
            raise
        
    
    async def all_country_sentiment(self):
        try:
            global country
            for ele in country:
                store = await self.news_sentiment(ele)
                
        except Exception as e:
            logging.error(f"Error caloculation all country sentiment: {e}", exc_info=True)
            raise
        
        
