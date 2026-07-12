

from pydantic import BaseModel


class ConfigType(BaseModel):
    supabase_url:str
    supabase_key:str
    news_api:str
    news_key:str
    db_server:str
    db_port:str
    db_name:str
    db_password:str
    db_user:str
    