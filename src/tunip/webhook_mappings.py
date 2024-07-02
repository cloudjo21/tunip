from typing import List
from pydantic import BaseModel


class WebHookMapping(BaseModel):
    channel: str
    webhook_url: str

class WebHookMappings(BaseModel):
    webhook_mapping: List[WebHookMapping]
