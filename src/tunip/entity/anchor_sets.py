from abc import ABC
from operator import attrgetter
import pyspark
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from typing import List

from .anchors import (
    Anchor
)

class AnchorSet(ABC):
    pass


class AnchorSet:
    def __init__(self, anchors: List[Anchor]):
        self.anchors = anchors
        self.distinct()
    
    def __iter__(self):
        return iter(self.anchors)
    
    def __str__(self):
        str_anchors = [str(anchor) for anchor in self.anchors]
        return ", ".join(str_anchors)
    
    def sort(self):
        self.anchors = sorted(self.anchors, key=attrgetter("anchor_text"))
        
    def distinct(self):
        # initialize
        self.sort()
        dis_anchors = [self.anchors[0]]
        for anchor in self.anchors:
            unique = True
            for d_anchor in dis_anchors:
                if anchor.__eq__(d_anchor):
                    unique = False
                    break
            if unique:
                dis_anchors.append(anchor)

        self.anchors = dis_anchors


anchor_set_schema = StructType([
        StructField("anchor_text", StringType()),
        StructField("context_token", StringType()),
        StructField("anchor_title", StringType()),
        StructField("anchor_first_sent", StringType()),
        StructField("anchor_parent_title", StringType()),
        StructField("anchor_parent_first_sent", StringType())
    ])