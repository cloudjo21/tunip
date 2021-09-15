import json
import random

from abc import ABC
from dataclasses import dataclass, field
from enum import Enum
from pprint import pprint, pformat
from termcolor import colored
from typing import List, Any

from tunip.Hangulpy import (
   is_hangul_jamo_char,
   convert_hangul_jamo_to_compatibility,
   concat_agglunated_tokens
)
from tunip.constants import NA_LABEL
from tunip.fuzzy_match import thresholded_ratio_between
from tunip.hash_utils import hash_func


@dataclass
class CorpusToken:
    start: int
    end: int
    pos: str
    surface: str

    @property
    def offset_mapping(self):
        return (self.start, self.end)

    def to_json(self):
        return [self.start, self.end, self.pos, self.surface]
    
    @classmethod
    def from_tuple_entry(cls, token_entry):
        return CorpusToken(
            start=token_entry[0],
            end=token_entry[1],
            pos=token_entry[2],
            surface=token_entry[3]
        )
    
    @classmethod
    def fit_into_split_words(cls, tokens: list) -> (list, list, list):
        """
        :param: tokens  list of instances of CorpusToken
        """
        for i, t in enumerate(tokens):
            for s in t.surface:
                if is_hangul_jamo_char(s[0]):
                    # 'ㅂ니다'
                    t.surface = convert_hangul_jamo_to_compatibility(s[0]) + s[1:]
        surfaces = [t.surface for t in tokens]

        # 때문 이 ㄴ지 => 때문 인지
        surfaces_updated, token_indices = concat_agglunated_tokens(surfaces)
        head_token_indices = [t[0] for t in token_indices]
        tokens_updated: list = [
            tokens[token_index] for token_index in head_token_indices
        ]
        return tokens_updated, token_indices, surfaces_updated


class CorpusLabel(ABC):
    pass


@dataclass
class CorpusSeqLabel(CorpusLabel):
    start: int
    end: int
    label: str

    @property
    def offset_mapping(self):
        return (self.start, self.end)

    def to_json(self):
        return [self.start, self.end, self.label]
    
    def to_tuple(self):
        return (self.start, self.end, self.label)

    @classmethod
    def from_tuple_entry(cls, label_entry):
        if label_entry:
            return CorpusSeqLabel(
                start=int(label_entry[0]),
                end=int(label_entry[1],
                label=label_entry[2]
            )
        else:
            return CorpusSeqLabel(-1, -1, NA_LABEL)
    
    def apply(self):
        return CorpusSeqLabel(-1, -1, NA_LABEL)
    
    def valid(self):
        return self.start < 0 or self.end < 0 or self.label == NA_LABEL


@dataclass
class CorpusInput:
    text: str
    tokens: list = None
    labels: list = None


@dataclass
class CorpusRecord:
    text: str
    labels: list
    tokens: list = field(default_factory=lambda: [])
    hash_value: int = -1

    def __hash__(self):
        return hash_func(self.text[:50])

    def to_json(self):
        return {
            'text': self.text,
            'labels': self.labels,
            'tokens': self.tokens
        }

    def update(self, other):
        self.text = other.text
        self.tokens = other.tokens
        self.labels = other.labels

    def update_labels(self, other):
        self.labels = other.labels


class CorpusRecordMaker:
    def __init__(self, input_columns, output_columns, out_json=True):
        self.input_columns = input_columns
        self.output_columns = output_columns
        self.out_json = out_json

    def __call__(self, row):
        record = CorpusRecord(
            text=row[self.input_columns[0]],
            tokens=row[self.input_columns[1]] if len(self.input_columns) > 1 else [],
            labels=row[self.output_columns[0]]
        )
        if self.out_json:
            return record.to_json()
        else:
            return record


class CorpusBuildColumnOperation(Enum):
    ADD = 0
    UPDATE = 1


@dataclass
class CorpusBuildRequest:
    entry: CorpusInput
    column_ops: List[CorpusBuildColumnOperation] = field(
        default_factory=lambda: [
            CorpusBuildColumnOperation.ADD,
            CorpusBuildColumnOperation.ADD
        ]
    )

class CorpusColumnTokenHandler:
    pass

class CorpusColumnTokensAdder:
    pass

class CorpusColumnTokensUpdater:
    pass

class CorpusColumnLabelsAdder:
    pass

class CorpusColumnLabelsUpdater:
    pass



@dataclass
class CorpusRecordPrecede50:
    text: str
    labels: list
    tokens: list = field(default_factory=lambda: [])
    tokens2: list = field(default_factory=lambda: [])
    hash_value: int = -1

    def __hash__(self):
        return hash_func(self.text[:50])

    def __eq__(self, other):
        return other.__hash__() == self.__hash__()

    def to_json(self):
        return {
            'text': self.text,
            'labels': self.labels,
            'tokens': self.tokens
        }


def get_corpus_tuple_entries(filepath):
    with open(filepath, encoding='utf-8') as f:
        for line in f:
            obj = json.loads(line, encoding='utf8')
            yield obj['text'], obj['tokens'], obj['labels']


def get_corpus_inputs(filepath):
    with open(filepath, encoding='utf-8') as f:
        for line in f:
            obj = json.loads(line, encoding='utf-8')
            tokens = obj['tokens'] if 'tokens' in obj else []
            labels = obj['labels'] if 'labels' in obj else []
            yield CorpusInput(
                text=obj['text'],
                labels=labels,
                tokens=tokens
            )


def get_corpus_records(filepath):
    with open(filepath, encoding='utf-8') as f:
        for line in f:
            obj = json.loads(line, encoding='utf-8')
            yield CorpusRecord(
                text=obj['text'],
                labels=obj['labels'],
                tokens=obj['tokens']
            )


def get_corpus_records_precede50(filepath):
    with open(filepath, encoding='utf-8') as f:
        for line in f:
            obj = json.loads(line, encoding='utf8')
            yield CorpusRecordPrecede50(
                text=obj['text'],
                labels=obj['labels'],
                tokens=obj['tokens']
            )


def get_non_entity_entries(rows, entity_type):
    """
    gather the entities which is not the entity tag as 'entity_type'
    """
    non_entity_entries = []
    for d in rows:

        text, _, labels = d

        for l in labels:
            beg, end, tag = l

            if entity_type not in tag:
                non_entity_entries.append(text[beg:end])

    return non_entity_entries


def get_text_generator_from_file(filepath):
    with open(filepath, mode='r', encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line, encoding='utf8')
            yield obj["text"]


def update_entries_with_entity_dic(rows, dic_entries, entity_type):
    """
    update the label of entity in the corpus by fuzzy matching similarity 
    between the entity of corpus and the one of keyword dictionary
    """

    for sent_entry in rows:
        entry = {}
        text, tokens, labels = sent_entry
        entry['text'] = text
        entry['tokens'] = tokens
        entry['labels'] = labels

        updated = []
        for l in labels:
            beg, end, tag = l

            # for d in dic_entries:
            for d in random.choices(dic_entries, k=100):
                entity = text[beg:end]

                # fuzzy match between dictionary entity and already tagged entity
                if thresholded_ratio_between(d.lower(), entity.lower()) is not None:
                # exact match between dictionary entity and already tagged entity
                # if d.lower() == entity.lower():

                    updated.append(colored('{}: {}=>{}'.format(entity, l[2], entity_type), 'green'))

                    l[2] = entity_type
                    break

        if len(updated) > 0:
            for u in updated:
                print(u)
            pprint(entry)

        yield entry


def convert_tokenized_inputs_to_corpus_tokens(
    tokenized_inputs,
    convert_ids_to_tokens
) -> List[List[CorpusToken]]:
    corpus_tokens_list = []
    offset_mappings = tokenized_inputs.pop("offset_mapping")
    input_ids = tokenized_inputs["input_ids"]

    for input_ids_entry, offset_mapping_entry in zip(
        input_ids,
        offset_mappings
    ):
        str_tokens = convert_ids_to_tokens(input_ids_entry)
        corpus_tokens = []
        for surface, offset in zip(str_tokens, offset_mapping_entry):
            corpus_token = CorpusToken(
                start=offset[0],
                end=offset[1],
                pos='',
                surface=surface
            )
            corpus_tokens.append(corpus_token)
        corpus_tokens_list.append(corpus_tokens)
    return corpus_tokens_list


def convert_corpus_tokens_to_offsets_mapping(
    corpus_tokens_list: List[List[CorpusToken]]
) -> List[Any]:
    """
    :return :  List[List[(int, int)]]
    """
    token_offsets_mapping_list = []
    for corpus_tokens in corpus_tokens_list:
        token_offsets_mapping = [ct.offset_mapping for ct in corpus_tokens]
        token_offsets_mapping_list.append(token_offsets_mapping)

    return token_offsets_mapping_list


def convert_examples_to_corpus_seq_labels(
    label_examples,
) -> List[List[CorpusSeqLabel]]:
    """
    :param label_examples:  list of [start, end, label] sequenece
    :return:    list of CorpusSeqLabel list
    """
    corpus_labels_list = []
    for label_entries in label_examples:
        corpus_labels = []
        for label_entry in label_entries:
            if label_entry:
                corpus_label = CorpusSeqLabel(
                    start=label_entry[0],
                    end=label_entry[1],
                    label=label_entry[2]
                )
            else:
                corpus_label = CorpusSeqLabel.from_tuple_entry(label_entry)
            corpus_labels.append(corpus_label)
        corpus_labels_list.append(corpus_labels)
    return corpus_labels_list


def convert_corpus_seq_labels_to_tuple_entries(
    corpus_labels_list
) -> List[list]:
    """
    """
    label_entries_list = []
    for corpus_labels in corpus_labels_list:
        label_entries = []
        for corpus_label in corpus_labels:
            label_entry = {
                "start": corpus_label.start,
                "end": corpus_label.end,
                "label": corpus_label.label
            }
            label_entries.append(label_entry)
        label_entries_list.append(label_entries)
    return label_entries_list
