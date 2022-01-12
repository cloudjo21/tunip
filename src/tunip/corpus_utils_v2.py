from typing import List, Optional
from pydantic import BaseModel
from pyspark.sql.types import Row

from tunip.gold import is_overlapped_token_on_label


def old_nugget_return_to_v2(old_ret):
    tokens = []
    for t in old_ret['tokens']:
        token = CorpusToken(start=t[0], end=t[1], pos=t[2], surface=t[3])
        tokens.append(token)
    return CorpusInput(text=old_ret['text'], tokens=tokens)


class CorpusSeqLabel(BaseModel):
    start: int
    end: int
    label: str


class CorpusToken(BaseModel):
    start: int
    end: int
    pos: str
    surface: str


class CorpusInput(BaseModel):
    text: str
    labels: Optional[List[CorpusSeqLabel]] = None
    tokens: Optional[List[CorpusToken]] = None


class CorpusRecord(BaseModel):
    text: str
    labels: List[CorpusSeqLabel]
    tokens: List[CorpusToken]


# print(
#     CorpusRecord(
#         text='aa',
#         labels=[CorpusSeqLabel(start=0, end=1, label='d')],
#         tokens=[
#             CorpusToken(start=0, end=1,pos='b', surface='c')]
#     )
# )