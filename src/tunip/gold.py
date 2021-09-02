import re
import spacy

from typing import Any, List

from spacy.tokens import Doc
from spacy.training import offsets_to_biluo_tags
from spacy.util import get_words_and_spaces
from spacy.vocab import Vocab

from tunip import nugget_utils
from tunip.corpus_utils import CorpusSeqLabel


def iob_from_biluo(labels):
    new_labels = []
    for lb in labels:
        lb = re.sub(r"U-", "B-", lb)
        lb = re.sub(r"L-", "I-", lb)
        new_labels.append(lb)

    return new_labels


def get_i_or_b_tag(cur_label_cnt):
    if cur_label_cnt[0] > 0:
        return f"I-{cur_label_cnt[1]}"
    elif cur_label_cnt[0] == 0:
        return f"B-{cur_label_cnt[1]}"
    else:
        raise ValueError(f"Invalid current label and count: {cur_label_cnt}")


def iob_from_tokens_and_labels(
    tokens: List[Any], labels: List[CorpusSeqLabel], outside_tag="O"
):
    """
    :param tokens:  list of token offsets for a sentence
                    e.g., [[start, end, ...], [start, end, ...], ...]
    :param labels:  list of instances of CorpusSeqLabel
                    including character offsets and label
    :return: label list aligned to tokens
    """
    iter_tokens = iter(tokens)
    iter_labels = iter(labels)

    labels_on_tokens = []
    has_next_label = True
    try:
        label = next(iter_labels)
    except StopIteration:
        has_next_label = False

    current_label_count = [0, label.label] if has_next_label is True else [-1, None]

    for token in iter_tokens:
        token_start, token_end = token[0], token[1]
        label_start, label_end = (
            (label.start, label.end) if has_next_label else (9999, -9999)
        )
        if has_next_label is False:
            labels_on_tokens.append(outside_tag)  # append 'O'utside tag
        elif token_end <= label_start:
            labels_on_tokens.append(outside_tag)  # append 'O'utside tag
        elif (label_start <= token_start and token_end <= label_end) or (
            token_start <= label_start and label_start < token_end
        ):
            ib_tag = get_i_or_b_tag(current_label_count)
            labels_on_tokens.append(ib_tag)
            current_label_count[0] += 1
        elif label_end <= token_start and has_next_label is True:
            try:
                label = next(iter_labels)
                current_label_count = [0, label.label]
                labels_on_tokens.append(outside_tag)  # append 'O'utside tag
            except StopIteration:
                has_next_label = False
                labels_on_tokens.append(outside_tag)  # append 'O'utside tag
                current_label_count = [-1, None]
        else:
            labels_on_tokens.append(outside_tag)  # append 'O'utside tag

    return labels_on_tokens


def _is_overlapped_token_on_label(token_start, token_end, label_start, label_end):
    return ((label_start <= token_start) and (token_end <= label_end)) or (
        (token_start <= label_start) and (label_start < token_end)
    )


def _next_token_offset(iter_tokens, prev_token=None, accum_token_end=0):
    token = next(iter_tokens, None)
    if token is None:
        token_offset = token
        has_next_token = False
    else:
        if not prev_token or (token[0] >= prev_token[0]) or (token[1] >= prev_token[1]):
            accum_token_end = 0
        token_offset = token[0] + accum_token_end, token[1] + accum_token_end
        has_next_token = True
    return token_offset, has_next_token


def iob_from_tokens_and_labels_v2(
    token_offsets: List[Any], labels: List[CorpusSeqLabel], outside_tag="O"
):
    """
    is_split_into_words is False
    :param tokens:  list of token offsets for a sentence
                    e.g., [[start, end, ...], [start, end, ...], ...]
    :param labels:  list of instances of CorpusSeqLabel
                    including character offsets and label
    :return: label list aligned to tokens
    """
    iter_tokens = iter(token_offsets)
    iter_labels = iter(labels)

    labels_on_tokens = []
    has_next_label = True
    has_next_token = True
    if len(labels) < 1:
        has_next_label = False
    try:
        if len(token_offsets) < 1:
            has_next_token = False
        else:
            token = next(iter_tokens)
            token_start, token_end = token[0], token[1]
    except StopIteration:
        has_next_label = False
        has_next_token = False

    if has_next_label is False:
        labels_on_tokens.extend(outside_tag * len(token_offsets))

    else:
        for label in iter_labels:

            current_label_count = (
                [0, label.label] if has_next_label is True else [-1, None]
            )
            label_start, label_end = (
                (label.start, label.end) if has_next_label else (9999, -9999)
            )

            while (token_end <= label_start) and has_next_token is True:
                labels_on_tokens.append(outside_tag)

                token_offset, has_next_token = _next_token_offset(iter_tokens)
                if has_next_token:
                    token_start, token_end = token_offset

            while has_next_token is True and _is_overlapped_token_on_label(
                token_start, token_end, label_start, label_end
            ):
                ib_tag = get_i_or_b_tag(current_label_count)
                labels_on_tokens.append(ib_tag)
                current_label_count[0] += 1

                token_offset, has_next_token = _next_token_offset(iter_tokens)
                if has_next_token:
                    token_start, token_end = token_offset

        while has_next_token is True:
            labels_on_tokens.append(outside_tag)
            _, has_next_token = _next_token_offset(iter_tokens)

    return labels_on_tokens


def update_offset_mappings_for_split_into_words(
    token_offsets: List[Any],
    word_ids: List[int],
    space_offsets: List[int]
):
    new_token_offsets = []
    prev_word_id = 0
    prev_offset_end = 0
    accum_token_end = 0
    for offsets, word_id in zip(token_offsets, word_ids):
        if word_id is None:
            continue
        if word_id and prev_word_id != word_id:
            accum_token_end = prev_offset_end
            if prev_offset_end in space_offsets:
                accum_token_end += 1
        elif not word_id:
            accum_token_end = 0
        offsets = (offsets[0] + accum_token_end, offsets[1] + accum_token_end)
        new_token_offsets.append(offsets)
        prev_word_id = word_id
        prev_offset_end = offsets[1]

    return new_token_offsets


def iob_from_labels_and_token_offsets(
    labels: List[tuple],
    token_offsets: List[list],
    return_offsets_mapping: bool = False,
    word_ids: List[int] = None,
    space_offsets: List[int] = None
):
    """
    return iob sequenced labels and its offsets for a sentence
    :labels:        list of tuple (start, end, label)
                    'labels' contains only valid tags wihtout outside tags, 
                    and 'labels' is not NOT IOB scheme
                    기아쏘렌토가격 => [(2, 5, 'CAR')]
    :token_offsets: offset mappings of tokens from the result of HuggingFace's Tokenizer
                    when is_split_into_words is False, [(0, 2), (2, 5), (5, 7)]
                    when is_split_into_words is True, [(0, 2), (0, 3), (0, 2)]
    :word_ids:      pass the word ids if is_split_into_words is True when you are using HuggingFace's Tokenizer
                    for each word, word_id is starting with zero.
                    기아쏘렌토가격 => 기아, 쏘렌토, 가격
                    when is_split_into_words is True, then word_ids=[1, 1, 1]
    """
    if word_ids is not None:
        assert space_offsets is not None
        token_offsets = update_offset_mappings_for_split_into_words(
            token_offsets, word_ids, space_offsets
        )
    seq_label = [CorpusSeqLabel.from_tuple_entry(label_tuple) for label_tuple in labels]
    label_mappings = iob_from_tokens_and_labels_v2(token_offsets, seq_label)
    if return_offsets_mapping is True:
        seq_label = [
            CorpusSeqLabel(offsets[0], offsets[1], label)
            for offsets, label in zip(token_offsets, label_mappings)
        ]
        return seq_label
    else:
        return label_mappings


def iob_from_corpus_entry(cp_entry):
    """
    :param cp_entry:        corpus_utils.CorpusRecord
    :return: iob schema based labels which is not including the character offsets
    """
    entdoc_words, entdoc_spaces = nugget_utils.get_words_and_spaces(
        # filter_overlapped(cp_entry.tokens)
        cp_entry.tokens
    )
    entdoc = Doc(Vocab(), words=entdoc_words, spaces=entdoc_spaces)
    bio_tags = offsets_to_biluo_tags(entdoc, cp_entry.labels)

    iob_labels = iob_from_biluo(bio_tags)
    return iob_labels


def iob_from_corpus_row(doc_text, words, labels):
    """
    :param doc_text:
    :param words:
    :param labels: list of [start_char, end_char, labels_]
    :return: iob schema based labels which is not including the character offsets
    """
    entdoc_words, entdoc_spaces = get_words_and_spaces(words, doc_text)

    entdoc = Doc(Vocab(), words=entdoc_words, spaces=entdoc_spaces)
    bio_tags = offsets_to_biluo_tags(entdoc, labels)

    iob_labels = iob_from_biluo(bio_tags)
    return iob_labels


def check_subsequent(id2label, prev_tag_id, curr_tag_id):
    """
    check whether two BIO tags are subsequent or not
    e.g.,
        - subsequent cases
        'B-PER, I-PER' is subsequent.
        'I-PER, I-PER' is subsequent.

        - NOT subsequent cases
        'I-PER, I-PER' is not subsequent.
        'B-PER, B-PER' is not subsequent.
        'B-PER, O' is not subsequent.
        'O, B-PER' is not subsequent.
    """
    ptag = id2label[prev_tag_id]
    ctag = id2label[curr_tag_id]
    if (ptag[2:] == ctag[2:]) and (
        (ptag[:2] == "B-" and ctag[:2] == "I-") or (ptag[:2] == "I-" and ctag[:2])
    ):
        return True
    else:
        return False


def check_start_ent(id2label, tag_id):
    tag = id2label[tag_id]
    if tag[:2] == "B-":
        return True
    else:
        return False


def ner_tag_filter_bi(id2label, tag_id):
    return id2label[tag_id][2:]
