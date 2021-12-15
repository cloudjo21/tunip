from dataclasses import dataclass

@dataclass
class DocumentRecord:
    title: str
    text: str

    def to_json(self):
        return {
            'title': self.title,
            'text': self.text
        }

    def update(self, other):
        self.title = other.title
        self.text = other.text
    
    @classmethod
    def from_json_entry(self, entry):
        record = DocumentRecord(
            title=entry["title"],
            text=entry["text"]
        )
        return record
    
    
@dataclass
class AnchorDocumentRecord:
    parent_title: str
    parent_text: str
    anchor_titles: str
    anchor_texts: str

    def to_json(self):
        return {
            'parent_title': self.parent_title,
            'parent_text': self.parent_text,
            'anchor_titles': self.anchor_titles,
            'anchor_texts': self.anchor_texts,
        }

    def update(self, other):
        self.parent_title = other.parent_title
        self.parent_text = other.parent_text
        self.anchor_titles = other.anchor_titles
        self.anchor_texts = other.anchor_texts
    
    @classmethod
    def from_json_entry(self, entry):
        record = AnchorDocumentRecord(
            parent_title=entry["parent_title"],
            parent_text=entry["parent_text"],
            anchor_titles=entry["anchor_titles"],
            anchor_texts=entry["anchor_texts"]
        )
        return record