from functools import lru_cache
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import logging
from dopplersdk import DopplerSDK
from dotenv import load_dotenv
from custom_types.config import ConfigType
import os


def doppler_secret()->ConfigType:
    try:
        load_dotenv()
        
        token = os.getenv("DOPPLER_TOKEN")
        
        if not token:
            raise RuntimeError(f"Failed to fetch Doppler secrets: {e}")
        
        doppler = DopplerSDK()
        doppler.set_access_token(token)
        
        return ConfigType(
            supabase_key= doppler.secrets.get(project="tpf_macro", config="dev",name="SUPABASE_KEY").value['raw'],
            supabase_url =  doppler.secrets.get(project="tpf_macro", config="dev",name="SUPABASE_URL").value['raw']
        )
        
        
    except Exception as e:
        logging.error(f"Error loading doppler secret")
        

@lru_cache     
def get_doppler_env():
    
    return doppler_secret()

