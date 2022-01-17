from .entity_corpus_record import *

from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType


token_schema = StructType(
    [
        StructField('start', IntegerType()),
        StructField('end', IntegerType()),
        StructField('tag', StringType()),
        StructField('surface', StringType())
    ]
)

label_schema = StructType(
    [
        StructField("start", IntegerType()),
        StructField("end", IntegerType()),
        StructField("tag", StringType()),
    ]
)

corpus_input_schema = StructType(
    [
        StructField('text', StringType()),
        StructField('tokens', ArrayType(token_schema)),
        StructField("labels", ArrayType(label_schema))
    ]
)
