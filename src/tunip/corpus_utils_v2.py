from typing import List, Optional
from pydantic import BaseModel

from tunip.gold import is_overlapped_token_on_label


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

    def add_or_not(self, other_label: CorpusSeqLabel):
        prev_label = None
        new_labels = []
        append_new_or_not = -1  #0: new 1: old
        label_replaced = False
        if self.labels:
            for i, label in enumerate(self.labels):
                if (other_label.start < label.start and label.start < other_label.end) and (not prev_label or prev_label.end <= other_label.start):
                    prev_label = other_label
                    self.labels[i] = other_label
                    label_replaced = True
                elif (other_label.start == label.start and label.end < other_label.end):
                    prev_label = other_label
                    self.labels[i] = other_label
                    label_replaced = True
                # elif label.end <= other_label.start:
                #     self.labels.insert(i+1, other_label)
                else:
                    prev_label = label
                    self.labels[i] = label
                    label_replaced = False
        else:
            self.labels = [other_label]
    
    def _len_of_label_surface(start, end):
        return len(text[start:end].replace(' ', ''))


# print(
#     CorpusRecord(
#         text='aa',
#         labels=[CorpusSeqLabel(start=0, end=1, label='d')],
#         tokens=[
#             CorpusToken(start=0, end=1,pos='b', surface='c')]
#     )
# )