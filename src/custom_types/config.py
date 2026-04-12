

from pydantic import BaseModel


class ConfigType(BaseModel):
    supabase_url:str
    supabase_key:str