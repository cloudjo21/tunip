from pydantic import BaseModel


class DocumentRecord(BaseModel):
    doc_id: int
    title: str
    text: str


class AnchorDocumentRecord(BaseModel):
    doc_id: int
    parent_title: str
    parent_text: str
    anchor_title: str
    anchor_text: str


class ClannDocumentRecord(BaseModel): 
    doc_id: int
    feature: str
    label: str