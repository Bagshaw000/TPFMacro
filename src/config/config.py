from functools import lru_cache
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import logging
from dopplersdk import DopplerSDK
from dotenv import load_dotenv
from custom_types.config import ConfigType



def doppler_secret()->ConfigType:
    try:
        load_dotenv()
        
        token = os.getenv("DOPPLER_TOKEN")
        
        if not token:
            
            logging.info(f"Failed to fetch Doppler secrets")
            raise RuntimeError(f"Failed to fetch Doppler secrets")
            
        
        doppler = DopplerSDK()
        doppler.set_access_token(token)
        
        return ConfigType(
            supabase_key= doppler.secrets.get(project="tpf_macro", config="dev",name="SUPABASE_KEY").value['raw'],
            supabase_url =  doppler.secrets.get(project="tpf_macro", config="dev",name="SUPABASE_URL").value['raw'],
            news_api= doppler.secrets.get(project="tpf_macro", config="dev",name="NEWS_API").value['raw'],
            news_key= doppler.secrets.get(project="tpf_macro", config="dev",name="NEWS_KEY").value['raw'],
            db_server = doppler.secrets.get(project="tpf_macro", config="dev", name="DB_SERVER").value['raw'],
            db_port = doppler.secrets.get(project="tpf_macro", config="dev", name="DB_PORT").value['raw'],
            db_name = doppler.secrets.get(project="tpf_macro", config="dev", name="DB_NAME").value['raw'],
            db_password = doppler.secrets.get(project="tpf_macro", config="dev", name="DB_PASSWORD").value['raw'],
            db_user = doppler.secrets.get(project="tpf_macro", config="dev", name="DB_USER").value['raw'],
            twitter_token= doppler.secrets.get(project="tpf_macro", config="dev", name="TWITTER_TOKEN").value['raw'],
            twitter_proxy= doppler.secrets.get(project="tpf_macro", config="dev", name="PROXY_SERVER").value['raw']
        )
        
        
    except Exception as e:
        logging.error(f"Error loading doppler secret: {e}")
        

@lru_cache     
def get_doppler_env():
    
    return doppler_secret()

