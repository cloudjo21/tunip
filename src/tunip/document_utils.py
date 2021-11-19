from dataclasses import dataclass, field
from tunip.hash_utils import hash_func

@dataclass
class DocumentRecord:
    title: str
    text: str
    hash_value: int = -1

    def __hash__(self):
        return hash_func(self.title[:50])

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