from abc import ABC
from operator import attrgetter
import pyspark
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from typing import List

from .spans import (
    SpanTitled
)

class SpanSet(ABC):
    pass


class SpanTitledSet(SpanSet):
    def __init__(self, spans: List[SpanTitled]):
        self.spans = spans
        self.distinct()
    
    def __iter__(self):
        return iter(self.spans)
    
    def __str__(self):
        str_spans = [str(span) for span in self.spans]
        return ", ".join(str_spans)
    
    def sort(self):
        self.spans = sorted(self.spans, key=attrgetter("span"))
        
    def distinct(self):
        # initialize
        self.sort()
        dis_spans = [self.spans[0]]
        for span in self.spans:
            unique = True
            for d_span in dis_spans:
                if span.__eq__(d_span):
                    unique = False
                    break
            if unique:
                dis_spans.append(span)

        self.spans = dis_spans
        
    @classmethod
    def from_dataframe_for_wiki(cls, dataframe: pyspark.sql.DataFrame) -> List[SpanTitled]:
        spans = []

        for row in range(len(dataframe)):
            span = dataframe["span"][row]
            span_titled = SpanTitled(
                span=span.span,
                span_type=span.span_type,
                title=span.title,
            )

            spans.append(span_titled)

        return spans       


span_titled_set_schema = StructType([
        StructField("span", StringType()),
        StructField("span_type", StringType()),
        StructField("title", StringType())
    ])