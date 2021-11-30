from abc import ABC
from dataclasses import dataclass

class NotEqualEntityLexcialException(Exception):
    pass


class Span(ABC):
    pass


@dataclass
class SpanTitled:
    span: str
    span_type: str
    title: str

    def __str__(self):
        return f"{self.span}/{self.span_type}/{self.title}"
