import re

from typing import List, Optional
from pydantic import BaseModel
from pyspark.sql.types import Row

from tunip.gold import is_overlapped_token_on_label
from tunip.nugget_utils import strip_spaces


def old_nugget_return_to_v2(old_ret):
    tokens = []
    old_tokens = strip_spaces(old_ret['tokens'])
    for t in old_tokens:
        token = CorpusToken(start=t[0], end=t[1], pos=t[2], surface=t[3])
        tokens.append(token)
    return CorpusInput(text=old_ret['text'], tokens=tokens, labels=old_ret['labels'])


class CorpusSeqLabel(BaseModel):
    start: int
    end: int
    label: str


class CorpusToken(BaseModel):
    start: int
    end: int
    pos: str
    surface: str


class CorpusTokenOnly(BaseModel):
    start: int
    end: int
    surface: str


class CorpusInput(BaseModel):
    text: str
    labels: Optional[List[CorpusSeqLabel]] = None
    tokens: Optional[List[CorpusToken]] = None


class CorpusRecord(BaseModel):
    text: str
    labels: List[CorpusSeqLabel]
    tokens: List[CorpusToken]


def merge_surface(tokens: List[CorpusToken], token_separator=' ', split_by_token=False):
    phrase_surface = [
        token_separator for _ in range(
            tokens[0].start, tokens[-1].end if not split_by_token else tokens[-1].end + len(tokens)
        )
    ]
    start = tokens[0].start
    for t in tokens:
        phrase_surface[t.start - start:t.end - start] = t.surface
        if split_by_token:
            start -= 1

    return re.sub(r'[ ]+', ' ', ''.join(phrase_surface).strip())


def merge_surface_filter_suffix(tokens: List[CorpusToken], token_separator=' ', suffix_pos_tags=['J']):
    phrase_surface = [
        token_separator for _ in range(
            tokens[0].start, tokens[-1].end
        )
    ]
    for t, nt in zip(tokens, tokens[1:] + [CorpusToken(start=tokens[-1].end+1, end=tokens[-1].end+1, pos='$', surface='$END')]):
        if (t.pos in suffix_pos_tags) and (t.end < nt.start):
            continue
        phrase_surface[t.start - tokens[0].start:t.end - tokens[0].start] = t.surface
    return re.sub(r'[ ]+', ' ', ''.join(phrase_surface).strip())


class CorpusTokenMerger:
    pass



class AdjTagCorpusTokenMerger(CorpusTokenMerger):
    """

        The parameter example in the case to allow token sequence merge
        for starting digits and the rest of digits and symbols

        allow_head_pos = ['SN']
        allow_pos_for_token_merge = ['SN', 'S']
        merged_pos = 'SN'
    """

    def __init__(self, allow_head_pos, allow_pos_for_token_merge, merged_pos):
        self.allow_head_pos = allow_head_pos
        self.allow_pos_for_token_merge = allow_pos_for_token_merge
        self.merged_pos = merged_pos

    def __call__(self, record: CorpusRecord) -> CorpusRecord:
        new_tokens = []
        sub_pattern = []
        first_pos = True
        for token in record.tokens:
            sub_pattern, first_pos = self._add_pattern_if_allowed(token, sub_pattern, first_pos)
            if first_pos and sub_pattern:
                new_text = merge_surface(sub_pattern, ' ')
                merged_token = CorpusToken(start=sub_pattern[0].start, end=sub_pattern[-1].end, pos=self.merged_pos, surface=new_text)
                new_tokens.append(merged_token)
                new_tokens.append(token)
                sub_pattern = []
                first_pos = True
            elif not sub_pattern:
                new_tokens.append(token)
            
        if sub_pattern:
            new_text = merge_surface(sub_pattern, ' ')
            merged_token = CorpusToken(start=sub_pattern[0].start, end=sub_pattern[-1].end, pos=self.merged_pos, surface=new_text)
            new_tokens.append(merged_token)
        
        new_record = CorpusRecord(text=record.text, labels=record.labels, tokens=new_tokens)
        return new_record

    def _add_pattern_if_allowed(self, token: CorpusToken, sub_pattern: list, first_pos: bool) -> tuple((list, bool)):
        if (token.pos in self.allow_head_pos) and not sub_pattern:
            sub_pattern.append(token)
            first_pos = False
        elif token.pos in self.allow_pos_for_token_merge and not first_pos:
            sub_pattern.append(token)
        else:
            first_pos = True
        return sub_pattern, first_pos


# print(
#     CorpusRecord(
#         text='aa',
#         labels=[CorpusSeqLabel(start=0, end=1, label='d')],
#         tokens=[
#             CorpusToken(start=0, end=1,pos='b', surface='c')]
#     )
# )