from .entity_corpus_record import *

from pyspark.sql.types import ArrayType, StringType, StructField, StructType


corpus_input_schema = StructType(
    [
        StructField('text', StringType()),
        StructField('tokens', ArrayType(self.token_schema))
    ]
)
