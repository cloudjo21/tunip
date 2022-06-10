import json
import nltk
import requests
import time

from enum import Enum
from typing import Union

from urllib.parse import urlparse, urlencode
from urllib.request import urlopen

from tunip.corpus_utils import get_text_generator_from_file, get_corpus_records
from tunip.corpus_utils_v2 import CorpusToken, CorpusTokenOnly, CorpusRecord, CorpusSeqLabel, CorpusInput
from tunip.logger import init_logging_handler_for_klass
from tunip.nugget_utils import strip_spaces
from tunip.preprocess import preprocess_korean, preprocess_tokens, preprocess_tokens_v2
from tunip.service_config import get_service_config


class NuggetFilterResultFormat(Enum):
    ALL = 0
    LEX = 1
    B_E_LEX = 2
    NUGGET = 3
    NUGGET_B_E_LEX = 4


class NotSupportNuggetFilterResultFormat(Exception):
    pass


class NuggetFilterToolsFactory:

    @classmethod
    def create(cls, result_format, nugget_api_obj):
        if result_format == NuggetFilterResultFormat.NUGGET:
            nugget_cls = CorpusToken
            return_nugget_func = nugget_api_obj._return_nugget
        elif result_format == NuggetFilterResultFormat.B_E_LEX:
            nugget_cls = None
            return_nugget_func = nugget_api_obj._return_b_e_lex
        elif result_format == NuggetFilterResultFormat.NUGGET_B_E_LEX:
            nugget_cls = CorpusTokenOnly
            return_nugget_func = nugget_api_obj._return_b_e_lex
        else:
            raise NotSupportNuggetFilterResultFormat(result_format)

        return nugget_cls, return_nugget_func


class NuggetFilter:

    @classmethod
    def return_nugget(cls, nugget, nugget_cls):
        return nugget_cls(**{key: nugget[i] for i, key in enumerate(nugget_cls.__fields__.keys())})

    @classmethod
    def return_nugget_b_e_lex(cls, nugget, nugget_cls):
        # nugget: CorpusToken
        return CorpusTokenOnly(start=nugget.start,end=nugget.end, surface=nugget.surface)

    @classmethod
    def return_b_e_lex(cls, nugget, nugget_cls):
        return [nugget.start, nugget.end, nugget.surface]


class NuggetFilterFuncProvider:

    @classmethod
    def apply(cls, result_format):
        if result_format == NuggetFilterResultFormat.NUGGET:
            nugget_cls = CorpusToken
            return_nugget_func = NuggetFilter.return_nugget

        elif result_format == NuggetFilterResultFormat.NUGGET_B_E_LEX or result_format == NuggetFilterResultFormat.B_E_LEX:
            nugget_cls = CorpusTokenOnly
            return_nugget_func = NuggetFilter.return_nugget_b_e_lex

        else:
            raise NotSupportNuggetFilterResultFormat(result_format)

        return nugget_cls, return_nugget_func


class Nugget:
    """
    Nugget API based Corpus Builder
    Corpus is made up of the list of CorpusRecord
    """

    def __init__(self, method="POST", tagger_type="seunjeon", use_inflect=False):
        self.logger = init_logging_handler_for_klass(klass=self.__class__)
        self.req_session = requests.Session()
        self.tagger_type = tagger_type
        self.method = method
        # self.chunk_size = 1 if self.method.lower() == 'get' else 100
        self.chunk_size = 1
        self.call_fn = self.get if self.method.lower() == "get" else self.post
        self.row_count = 0
        self.use_inflect = use_inflect
        self.nugget_host = get_service_config().config['nugget.host']

    def __del__(self):
        self.req_session.close()

    def __call__(self, texts: Union[list, str]):
        if isinstance(texts, str):
            texts = [texts]
        return self.record(texts)

    def build_from_file(self, jsonl_filepath, line_to_resume=0):
        print(jsonl_filepath)
        text_gen = get_text_generator_from_file(jsonl_filepath)
        self.row_count = 0
        chunks = []
        for text in text_gen:
            # print(text)
            self.row_count += 1

            if line_to_resume > self.row_count:
                continue

            # if self.row_count == 11:
            #     break

            chunks.append(preprocess_korean(text))

            if self.row_count % self.chunk_size == 0:
                res = self.call_fn(chunks)
                time.sleep(0.05)
                if res.status_code == 200:
                    res_json = res.json()
                    tokens = []
                    # (start, end, label)
                    # parse sentences
                    for txt, res_sent in zip(chunks, res_json["sentences"]):
                        tokens = []
                        entities = []
                        for res_token in res_sent["tokens"]:
                            tokens.append(
                                [
                                    res_token["begin"],
                                    res_token["end"],
                                    res_token["pos"],
                                    res_token["surface"],
                                ]
                            )
                        for res_token in res_sent["entities"]:
                            entities.append(
                                (res_token["begin"], res_token["end"], res_token["tag"])
                            )
                        entry = {}
                        entry["labels"] = entities
                        entry["text"] = txt
                        entry["tokens"] = strip_spaces(tokens)

                        yield entry

                chunks = []

        res = self.call_fn(chunks)
        if res.status_code == 200:
            res_json = res.json()
            tokens = []
            # (start, end, label)
            # parse sentences
            for txt, res_sent in zip(chunks, res_json["sentences"]):
                tokens = []
                entities = []
                for res_token in res_sent["tokens"]:
                    tokens.append(res_token["surface"])
                for res_token in res_sent["entities"]:
                    entities.append(
                        (res_token["begin"], res_token["end"], res_token["tag"])
                    )
                entry = {}
                entry["labels"] = entities
                entry["text"] = txt
                entry["tokens"] = strip_spaces(tokens)

                # sents.append(entry)
                yield entry


    def build_from_file_with_labels(self, jsonl_filepath, line_to_resume=0):
        print(jsonl_filepath)
        record_gen = get_corpus_records(jsonl_filepath)
        self.row_count = 0
        chunks = []
        for record in record_gen:
            # print(text)
            self.row_count += 1

            if line_to_resume > self.row_count:
                continue

            # if self.row_count == 11:
            #     break

            chunks.append(preprocess_korean(record.text))
            # chunks.append(text)

            if self.row_count % self.chunk_size == 0:
                res = self.call_fn(chunks)
                time.sleep(0.02)
                if res.status_code == 200:
                    res_json = res.json()
                    tokens = []
                    # (start, end, label)
                    # parse sentences
                    for txt, res_sent in zip(chunks, res_json["sentences"]):
                        tokens = []
                        for res_token in res_sent["tokens"]:
                            tokens.append(
                                [
                                    res_token["begin"],
                                    res_token["end"],
                                    res_token["pos"],
                                    res_token["surface"],
                                ]
                            )
                        # entities = []
                        # for res_token in res_sent["entities"]:
                        #     entities.append(
                        #         (res_token["begin"], res_token["end"], res_token["tag"])
                        #     )
                        entry = {}
                        entry["labels"] = record.labels
                        entry["text"] = txt
                        entry["tokens"] = strip_spaces(tokens)

                        yield entry

                chunks = []

        res = self.call_fn(chunks)
        if res.status_code == 200:
            res_json = res.json()
            tokens = []
            # (start, end, label)
            # parse sentences
            for txt, res_sent in zip(chunks, res_json["sentences"]):
                tokens = []
                for res_token in res_sent["tokens"]:
                    tokens.append(res_token["surface"])
                # entities = []
                # for res_token in res_sent["entities"]:
                #     entities.append(
                #         (res_token["begin"], res_token["end"], res_token["tag"])
                #     )
                entry = {}
                entry["labels"] = record.labels
                entry["text"] = txt
                entry["tokens"] = strip_spaces(tokens)

                # sents.append(entry)
                yield entry


    def record(self, texts):
        texts = [preprocess_korean(text) for text in texts]
        res = self.call_fn(texts)
        if res.status_code == 200:
            res_json = res.json()
            tokens = []
            # (start, end, label)
            # parse sentences
            for txt, res_sent in zip(texts, res_json["sentences"]):
                tokens = []
                entities = []
                for res_token in res_sent["tokens"]:
                    # tokens.append(res_token['surface'])
                    tokens.append(
                        [
                            res_token["begin"],
                            res_token["end"],
                            res_token["pos"],
                            res_token["surface"],
                        ]
                    )
                for res_token in res_sent["entities"]:
                    entities.append(
                        (res_token["begin"], res_token["end"], res_token["tag"])
                    )
                entry = {}
                entry["labels"] = entities
                entry["text"] = txt
                entry["tokens"] = strip_spaces(tokens)

                # sents.append(entry)
                yield entry


    def record_v2(self, texts):
        texts = [preprocess_korean(text) for text in texts]
        res = self.call_fn(texts)
        if res.status_code == 200:
            res_json = res.json()

            for txt, res_sent in zip(texts, res_json["sentences"]):
                tokens = []
                labels = []
                for res_token in res_sent["tokens"]:
                    tokens.append(
                        CorpusToken.parse_obj(
                            {'start': res_token['begin'], 'end': res_token['end'], 'pos': res_token['pos'], 'surface': res_token['surface']}
                        )
                    )
                for res_label in res_sent['entities']:
                    labels.append(
                        CorpusSeqLabel.parse_obj(
                            {'start': res_label['begin'], 'end': res_label['end'], 'label': res_label['tag']}
                        )
                    )
                yield CorpusRecord(text=txt, tokens=tokens, labels=labels)


    def bigrams(self, texts, white_tags, result_format):
        res_bigrams = []
        records_origin = self.record_v2(texts)
        records = self.filter_v2(
            nuggets=list(records_origin),
            white_tags=white_tags,
            result_format=result_format
        )
        # [['삼성', '전자', '연구소'], ['늘', '푸른', '법률', '사무소']]
        # res_bigrams: [[('삼성', '전자'), ('전자', '연구소')], [('늘', '푸른'), ('푸른', '법률'), ('법률', '사무소')]]
        for record in records:
            unigrams = [t.surface for t in record]
            res_bigrams.append(list(nltk.bigrams(unigrams)))
        return res_bigrams


    def bigrams_also(self, texts, white_tags, result_format):
        res_bigrams = []
        res_unigrams = []
        records_origin = self.record_v2(texts)
        records = self.filter_v2(
            nuggets=list(records_origin),
            white_tags=white_tags,
            result_format=result_format
        )
        # [['삼성', '전자', '연구소'], ['늘', '푸른', '법률', '사무소']]
        # res_bigrams: [[('삼성', '전자'), ('전자', '연구소')], [('늘', '푸른'), ('푸른', '법률'), ('법률', '사무소')]]
        for record in records:
            unigrams = [t.surface for t in record]
            res_bigrams.append(list(nltk.bigrams(unigrams)))
            res_unigrams.append(unigrams)
        return res_bigrams, res_unigrams


    def post(self, texts):
        body = {
            "taggerType": self.tagger_type,
            "text": "\n".join(texts),
            "splitSentence": True,
            "useInflect": self.use_inflect
            # "splitSentence": False
        }
        res = self.req_session.post(
            url=f"{self.nugget_host}/tagging/bulk", json=body
        )
        return res

    def get(self, text):
        params = urlencode({"tagger_type": self.tagger_type, "text": text, "use_inflect": self.use_inflect})
        res = self.req_session.get(
            url=f"{self.nugget_host}/tagging?{params}"
        )
        if res.status_code == 200:
            res_json = res.json()
            tokens = []
            # (start, end, label)
            entities = []
            print(f"#### {res_json}")
            for res_token in res_json["tokens"]:
                tokens.append(res_token["surface"])
            for res_token in res_json["entities"]:
                entities.append(
                    (res_token["begin"], res_token["end"], res_token["tag"])
                )
            entry = {}
            entry["labels"] = entities
            entry["text"] = text
            entry["tokens"] = strip_spaces(tokens)

            return entry
        else:
            return None
    
    def filter(self, nuggets, white_tags=[], result_format=NuggetFilterResultFormat.ALL):
        nugget_cls, return_nugget_func = NuggetFilterToolsFactory.create(result_format, self)

        nugget_tokens = []
        for tokens in preprocess_tokens(list(nuggets), white_tags=white_tags):
            nugget_tokens.append([return_nugget_func(n, nugget_cls) for n in tokens])

        return nugget_tokens
    
    def filter_v2(self, nuggets, white_tags=[], result_format=NuggetFilterResultFormat.ALL):
        nugget_cls, return_nugget_func = NuggetFilterFuncProvider.apply(result_format)

        nugget_tokens = []
        for tokens in preprocess_tokens_v2(list(nuggets), white_tags=white_tags):
            nugget_tokens.append([return_nugget_func(n, nugget_cls) for n in tokens])

        return nugget_tokens

    def _return_nugget(self, nugget, nugget_cls):
        return nugget_cls(**{key: nugget[i] for i, key in enumerate(nugget_cls.__fields__.keys())})

    def _return_nugget_b_e_lex(self, nugget, nugget_cls):
        return CorpusTokenOnly(start=nugget[0],end=nugget[1], surface=nugget[3])

    def _return_b_e_lex(self, nugget, nugget_cls):
        return [nugget[0], nugget[1], nugget[3]]

    def test_call_seunjeon(self):
        params = urlencode(
            {
                "tagger_type": "seunjeon",
                "text": '"KB국민은행 스타뱅킹"앱을 설치하고 접속한 다음, 화면 오른쪽 상단에 보이는 " 전체메뉴"로 이동해 준다.',
            }
        )
        print(params)
        res = self.req_session.get(
            url=f"{self.nugget_host}/tagging?{params}"
        )
        if res.status_code == 200:
            res_json = res.json()
            print(json.dumps(res_json, indent=4))

    def test_call_etri(self):
        params = urlencode(
            {
                "tagger_type": "etri",
                "text": '"KB국민은행 스타뱅킹"앱을 설치하고 접속한 다음, 화면 오른쪽 상단에 보이는 " 전체메뉴"로 이동해 준다.',
            }
        )
        print(params)
        res = self.req_session.get(
            url=f"{self.nugget_host}/tagging?{params}"
        )
        if res.status_code == 200:
            res_json = res.json()
            print(json.dumps(res_json, indent=4))

    def test_post_call_to_etri(self, text):
        body = {
            "access_key": "959129f3-7dec-4088-a307-ecac4a2be3bb",
            "argument": {
                # "text": "\"복잡한 대출\", 우리는 간편한도조회부터 시작! 연4.7-19.7% 최대 5천만\n아름다운 우리강산",
                # "text": "밥을 먹었다. 산책을 간다.",
                "text": text,
                "analysis_code": "ner",
            },
        }
        url = "http://aiopen.etri.re.kr:8000/WiseNLU"
        res = self.req_session.post(url=url, json=body)
        print(json.dumps(res.json(), indent=4, ensure_ascii=False))

    def test_post_call_to_nugget(self, text, tagger_type):
        body = {
            "taggerType": tagger_type,
            # "text": "\"복잡한 대출\", 우리는 간편한도조회부터 시작! 연4.7-19.7% 최대 5천만원\n아름다운 금수강산",
            "text": text,
            "splitSentence": True,
            "useInflect": False,
        }
        res = self.req_session.post(
            url=f"{self.nugget_host}/tagging/bulk", json=body
        )
        print(json.dumps(res.json(), indent=4, ensure_ascii=False))


if __name__ == "__main__":
    ap = Nugget(tagger_type="seunjeon")
    # entries = ap(jsonl_filepath='/data1/home/yh.cho/temp/ner/data/jsonl/kr_kb_80000/split/corpus.aa.jsonl')
    # print(len(list(entries)))
    # ap.test_call_seunjeon()
    # ap.test_post_call_to_etri()
    # ap.test_post_call_to_nugget()
    text = "현금”이 필요할때 보유한 주식을 담보로 대출받는 서비스 증권담보대출이란? 보유한 주식을 담보로 최대 10억원까지 현금 ..."
    response = list(ap.record([text]))
    # ap.test_post_call_to_etri(text)
    # ap.test_post_call_to_nugget(text, "seunjeon")
    response = list(ap.record([text]))
    print("#### response:")
    print(response)
    
    text = "1. 육군인쇄창 부지 ꡒ 병풍아파트 ꡓ 의혹"
    response = list(ap.record([text]))
    print("#### response:")
    print(response)

    text = "무하지룬(Muhajirun, المهاجرون‎)은 헤지라 때"
    expect = "무하지룬(Muhajirun, )은 헤지라 때"
    response = list(ap.record([text]))
    print("#### response:")
    print(response)
    assert response[0]['text'] == expect
    surfaces = [t[3] for t in response[0]['tokens']]
    assert 'المهاجرون' not in surfaces
    assert '\u200e' not in surfaces
    

    text = "Bork는 러시아의 가전 ​​제품 제조 회사이자"
    expect = "Bork는 러시아의 가전 제품 제조 회사이자"
    response = list(ap.record([text]))
    print("#### response:")
    print(response)
    assert response[0]['text'] == expect
    surfaces = [t[3] for t in response[0]['tokens']]
    assert '\u200b\u200b' not in surfaces
