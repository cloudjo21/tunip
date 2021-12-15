from pydantic import BaseModel

class DocumentRecord(BaseModel):
    title: str
    text: str
    
class AnchorDocumentRecord(BaseModel):
    parent_title: str
    parent_text: str
    anchor_title: str
    anchor_text: str