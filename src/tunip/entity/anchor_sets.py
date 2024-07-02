from abc import ABC
from operator import attrgetter
import pyspark
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from typing import List

from .anchors import (
    Anchor,
    AnchorInstance
)

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
        self.anchors = sorted(self.anchors, key=attrgetter("anchor_title"))
        
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

class AnchorInstanceSet:
    def __init__(self, instances: List[AnchorInstance]):
        self.instances = instances
        self.distinct()
    
    def __iter__(self):
        return iter(self.instances)
    
    def __str__(self):
        str_instances = [str(instance) for instance in self.instances]
        return ", ".join(str_instances)
    
    def sort(self):
        self.instances = sorted(self.instances, key=attrgetter("anchor_text"))
        
    def distinct(self):
        # initialize
        self.sort()
        dis_instances = [self.instances[0]]
        for instance in self.instances:
            unique = True
            for d_instance in dis_instances:
                if instance.__eq__(d_instance):
                    unique = False
                    break
            if unique:
                dis_instances.append(instance)

        self.instances = dis_instances
        
        
anchor_set_schema = StructType([
        StructField("id", IntegerType()),
        StructField("anchor_title", StringType()),
        StructField("anchor_first_sent", StringType()),
        StructField("anchor_parent_title", StringType()),
        StructField("anchor_parent_first_sent", StringType())
    ])


anchor_instance_set_schema = StructType([
        StructField("anchor_title_id", IntegerType()),
        StructField("anchor_text", StringType()),
        StructField("context_token", StringType())
    ])