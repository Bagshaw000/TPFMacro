from datetime import datetime
import sys
import os
from typing import List

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import asyncio
import logging
from Scweet import Scweet, ScweetConfig, ScweetDB
from config.config import get_doppler_env
import pandas as pd

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer



country=["US","CA","JP","DE","UK","AU","IN","CN","KR","BR","FR"]



class TwitterController:
    
    def __init__(self):
        self.scweet = Scweet(auth_token=get_doppler_env().twitter_token, db_path="scweet_state.db",config=ScweetConfig(
        concurrency=3,
        proxy=get_doppler_env().twitter_proxy,
        min_delay_s=2.0,
    ),)
    
    
    async def scrape_tweet(self, search:str, country:str, semaphore: asyncio.Semaphore):
        async with semaphore:
            try:
                today = datetime.now()
                await asyncio.sleep(120)
                # Format as YYYY-MM-DD
                date_string = today.strftime("%Y-%m-%d")
                tweets = await self.scweet.asearch(search, since=date_string ,lang="en",limit=150, min_likes=200, blue_verified_only=True, min_retweets=50, verified_only=True)
                print(tweets)
                print(country)
                
                
                return tweets
            except Exception as e:
                logging.error(f"Issue scraping tweet for {country}: {e}", exc_info=True)
                raise 
    
    async def active_sentiment(self):
        try:
            tweets = [
                '((US OR USA OR "United States") ($SPY OR $DIA OR $AAPL OR $MSFT) (economy OR finance OR recession OR inflation OR GDP OR "interest rates" OR jobs OR "Federal Reserve")) OR (from:elonmusk (economy OR market OR stock)) OR (("bullish" OR "bearish" OR "overvalued" OR "catalyst") (economy OR market))',
                '(Canada OR CA) ($TSX OR $CNQ OR $RY) (economy OR finance OR recession OR inflation OR GDP OR "interest rates" OR jobs OR "housing market" OR "Bank of Canada")',
                '(UK OR "United Kingdom") ($FTSE OR $HSBA OR $VOD) (economy OR finance OR recession OR inflation OR GDP OR "interest rates" OR jobs OR "Bank of England" OR "sterling")',
                '(Australia OR AU) ($ASX OR $BHP OR $CSL) (economy OR finance OR recession OR inflation OR GDP OR "interest rates" OR jobs OR "RBA" OR "AUD")',
                '(China OR CN) ($FXI OR $BABA OR $TCEHY) (economy OR finance OR recession OR inflation OR GDP OR "interest rates" OR jobs OR "PBOC" OR "yuan")'
            ]
            
            sem = asyncio.Semaphore(1)
            us_tweets, ca_tweets, uk_tweets, au_tweets, cn_tweets = await asyncio.gather(
                        self.scrape_tweet(tweets[0], "US", sem),
                        self.scrape_tweet(tweets[1], "CA", sem),
                        self.scrape_tweet(tweets[2], "UK", sem),
                        self.scrape_tweet(tweets[3], "AU", sem),
                        self.scrape_tweet(tweets[0], "CN", sem)
                           )
        
            us_senti, ca_senti, uk_senti, au_senti, cn_senti = await asyncio.gather(
                                self.extract_sentiment(us_tweets, "US"),
                                self.extract_sentiment(ca_tweets, "CA"),
                                self.extract_sentiment(uk_tweets, "UK"),
                                self.extract_sentiment(au_tweets, "AU"),
                                self.extract_sentiment(cn_tweets, "CN")
                                    )
            
            
        except Exception as e:
            logging.error(f"Error getting tweet : {e}", exc_info=True)
            raise 
        
    async def extract_sentiment(self, tweet:List[dict], country:str):
        try:
            # nltk.download('vader_lexicon')
            analyzer = SentimentIntensityAnalyzer()

            # global analyzer
            
            df = pd.DataFrame([await self.extract_tweet_info(t) for t in tweet])
            
            all_text = '. '.join(df['text'].astype(str))
            
            
            sentiment_score = analyzer.polarity_scores(all_text)
            print(country)
            print(sentiment_score["compound"])
            
            # Store sentiment for all economies in redis and database
                
            
        except Exception as e:
            logging.error(f"Error extracting sentiment: {e}", exc_info=True)
            raise
    
    async def extract_tweet_info(self,tweet):
        """Extract long text and metadata from a tweet"""
        try:
            text = tweet['data']['note_tweet']['note_tweet_results']['result']['text']
        except:
            text = tweet['data']['legacy']['full_text']
        
        return text
    
            
# test = TwitterController()


# loop = asyncio.get_event_loop()

# if loop.is_running():
#     # If loop is already running, schedule the coroutine
#     # nltk.download('vader_lexicon')
#     val = asyncio.create_task(test.active_sentiment())
#     print(val)
# else:
#     # If no loop is running, run it synchronously
#     val = asyncio.run(test.active_sentiment())
#     print(val)