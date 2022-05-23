from abc import ABC, abstractmethod
from typing import Iterator
from dataclasses import dataclass
from typing import List

from tunip.corpus_utils import (
    CorpusInput,
    CorpusRecord,
    CorpusToken,
    CorpusSeqLabel,
    convert_corpus_seq_labels_to_tuple_entries,
    convert_examples_to_corpus_seq_labels,
    convert_tokenized_inputs_to_corpus_tokens,
    get_corpus_inputs
)
from tunip.service_config import get_service_config


@dataclass
class EntityCorpusRecorderCreateRequest:
    builder_type: str
    padding: bool = False
    tagger_type: str = 'seunjeon'
    http_reqest_type: str = 'POST'


class EntityCorpusRecorder(ABC):

    @abstractmethod
    def record(
        self,
        inputs: List[CorpusInput],
        golden_labels: List[list] = None
    ) -> Iterator[CorpusRecord]:
        """
        :param texts:  list of text
        :param golden_labels:   we can inject the external golden label annotations,
                                or it can be carried on the instance of CorpusRecorder
                                List[List[int, int, str]]
        """
        pass


    @abstractmethod
    def build_from_file(self, jsonl_file_path: str, *args) -> Iterator[CorpusRecord]:
        pass


class TokenizerBasedEntityCorpusRecorder(EntityCorpusRecorder):

    def __init__(self, tokenizer, **kwargs):
        super(TokenizerBasedCorpusRecorder, self).__init__(**kwargs)
        self.tokenizer = tokenizer
        if 'padding' in kwargs.keys():
            self.padding = kwargs['padding']
        else:
            self.padding = False

    def record(
        self,
        inputs: List[CorpusInput],
        golden_labels: List[list] = None
    ) -> Iterator[CorpusRecord]:

        texts = [input_.text for input_ in inputs]
        tokenized_inputs = self.tokenizer(
            texts,
            padding=self.padding,
            # truncation=True,
            # We use this argument because the texts in our dataset are lists of words (with a label for each word).
            is_split_into_words=True,
            return_special_tokens_mask=True,
            return_offsets_mapping=True
        )
        # List[List[CorpusToken]]
        corpus_tokens_list: List[list] = convert_tokenized_inputs_to_corpus_tokens(
            tokenized_inputs=tokenized_inputs,
            convert_ids_to_tokens=self.tokenizer.convert_ids_to_tokens,
        )

        # label_entries_list = convert_corpus_seq_labels_to_tuple_entries([input_.labels for input_ in inputs])
        # corpus_seq_labels_list: List[List[CorpusSeqLabel]] = convert_examples_to_corpus_seq_labels(
        #     label_entries_list
        # )

        for input_, corpus_tokens in zip(
            # texts,
            inputs,
            corpus_tokens_list
            # corpus_seq_labels_list
        ):
            record = CorpusRecord(
                text=input_.text,
                tokens=corpus_tokens,
                labels=input_.labels
            )
            yield record


    def build_from_file(self, jsonl_file_path: str, *args) -> Iterator[CorpusRecord]:
        inputs = get_corpus_inputs(jsonl_file_path)
        return self.record(inputs)


class NuggetBasedEntityCorpusRecorder(EntityCorpusRecorder):
    """
    annotate entity labels by 'entities' from the result of nugget api
      - dictionary based entities from nugget api for tagger_type=seunjeon
    """

    def __init__(self, **kwargs):
        super(NuggetBasedEntityCorpusRecorder, self).__init__(**kwargs)
        self.tagger_type = kwargs['tagger_type'] if 'tagger_type' in kwargs.keys() else 'seunjeon'
        self.method = kwargs['method']
        self.call_fn = self._get if self.method.lower() == 'get' else self._post

        self.use_inflect = kwargs.get("use_inflect") or False
        self.elastic_host = get_service_config['elastic.host']


    def record(
        self,
        inputs: List[CorpusInput],
        golden_labels: List[list] = None
    ) -> Iterator[CorpusRecord]:
        texts = [input_.text for input_ in inputs]
        res = self.call_fn(texts)
        if res.status_code == 200:
            res_json = res.json()
            tokens = []
            # (start, end, label)
            # parse sentences
            for input_, res_sent in zip(inputs, res_json['sentences']):
                tokens = []
                entities = []
                for res_token in res_sent['tokens']:
                    token = CorpusToken(
                        start=res_token['begin'],
                        end=res_token['end'],
                        pos=res_token['pos'],
                        surface=res_token['surface']
                    )
                    tokens.append([token])

                for res_token in res_sent['entities']:
                    entity = CorpusSeqLabel(
                        start=res_token['begin'],
                        end=res_token['end'],
                        label=res_token['tag']
                    )
                    entities.append(entity)

                record_entry = CorpusRecord(
                    text=input_.text,
                    tokens=tokens,
                    labels=entities
                )

                yield record_entry


    def build_from_file(self, jsonl_file_path: str, *args) -> Iterator[CorpusRecord]:
        raise NotImplementedError()

    def _post(self, texts):
        body = {
            "taggerType": self.tagger_type,
            "text": "\n".join(texts),
            "splitSentence": True,
            "useInflect": self.use_inflect
        }
        res = self.req_session.post(
            url=f"{self.elastic_host}/tagging/bulk",
            json=body
        )
        return res
    
    def _get(self, text):
        params = urlencode({"tagger_type": self.tagger_type, "text": text, "use_inflect": self.use_inflect})
        res = self.req_session.get(
            url=f"{self.elastic_host}/tagging?{params}"
        )
        if res.status_code == 200:
            res_json = res.json()
            tokens = []
            # (start, end, label)
            entities = []
            for res_token in res_json['tokens']:
                tokens.append(res_token['surface'])
            for res_token in res_json['entities']:
                entities.append(
                    (res_token['begin'], res_token['end'], res_token['tag'])
                )
            entry = {}
            entry["labels"] = entities
            entry["text"] = text
            entry["tokens"] = tokens

            return entry
        else:
            return None
