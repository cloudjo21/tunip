from dataclasses import dataclass, field
from tunip.hash_utils import hash_func

@dataclass
class SpanRecord:
    title: str
    spans: list
    hash_value: int = -1

    def __hash__(self):
        return hash_func(self.title[:50])

    def to_json(self):
        return {
            'title': self.title,
            'spans': self.spans
        }

    def update(self, other):
        self.title = other.title
        self.spans = other.spans
        
    def update_spans(self, other):
        self.spans = other.spans
    
    @classmethod
    def from_json_entry(self, entry):
        record = SpanRecord(
            title=entry["title"],
            spans=entry["spans"]
        )
        return record