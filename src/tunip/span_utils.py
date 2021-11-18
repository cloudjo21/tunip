from dataclasses import dataclass, field
from tunip.hash_utils import hash_func

@dataclass
class SpanRecord:
    span: str
    span_type: str
    hash_value: int = -1

    def __hash__(self):
        return hash_func(self.span[:50])

    def to_json(self):
        return {
            'span': self.span,
            'span_type': self.span_type
        }

    def update(self, other):
        self.span = other.span
        self.span_type = other.span_type
    
    @classmethod
    def from_json_entry(self, entry):
        record = SpanRecord(
            span=entry["span"],
            span_type=entry["span_type"]
        )
        return record
    
@dataclass
class MentionRecord:
    mention: str
    title: str
    hash_value: int = -1

    def __hash__(self):
        return hash_func(self.mention[:50])

    def to_json(self):
        return {
            'mention': self.mention,
            'title': self.title
        }

    def update(self, other):
        self.mention = other.mention
        self.title = other.title
    
    @classmethod
    def from_json_entry(self, entry):
        record = MentionRecord(
            mention=entry["mention"],
            title=entry["title"]
        )
        return record