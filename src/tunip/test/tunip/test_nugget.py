import json
import re
import unittest
from unittest import result

from tunip.Hangulpy import is_hangul
from tunip.corpus_utils_v2 import CorpusRecord, CorpusToken, CorpusTokenOnly
from tunip.nugget_api import Nugget, NuggetFilterResultFormat
from tunip.preprocess import preprocess_korean

class NuggetTest(unittest.TestCase):
    def setUp(self):
        self.nugget = Nugget(tagger_type="seunjeon", split_sentence=True)

    def test_record(self):
        text = "오늘 아침 학교 가는 버스안에서\n학교 가자\n지미 카터"
        response = list(self.nugget(text.splitlines()))
        assert len(response) > 0

        text = "학교 가자"
        response = list(self.nugget.record([text]))
        assert len(response) > 0

        text = "지미 카터"
        response = list(self.nugget.record([text]))
        assert len(response) > 0

    def test_filter(self):
        self.nugget = Nugget(tagger_type="seunjeon", split_sentence=False)

        text = "오늘 아침 학교 가는 버스안에서\n학교 가자\n지미 카터"
        nuggets = list(self.nugget.record([text]))
        tokens = self.nugget.filter(
            nuggets, white_tags=["N", "V"], result_format=NuggetFilterResultFormat.NUGGET
        )

        expected_tokens = [
            [3, 5, "N", "아침"],
            [10, 12, "N", "학교"],
            [20, 21, "V", "가"],
            [22, 24, "N", "버스"],
            [27, 28, "N", "안"],
            [30, 32, "N", "학교"],
            [37, 38, "V", "가"],
            [40, 42, "N", "카터"],
        ]
        expected = [
            CorpusToken(
                **{
                    key: expected_tokens[j][i]
                    for i, key in enumerate(CorpusToken.__fields__.keys())
                }
            )
            for j in range(len(expected_tokens))
        ]
        assert tokens[0] == expected

    def test_filter_with_B_E_LEX(self):
        text = "오늘 아침 학교 가는 버스안에서\n학교 가자\n지미 카터"
        nuggets = list(self.nugget.record([text]))
        tokens = self.nugget.filter(
            nuggets, white_tags=["N", "V"], result_format=NuggetFilterResultFormat.B_E_LEX
        )

        expected_tokens = [
            [3, 5, "아침"],
            [10, 12, "학교"],
            [20, 21, "가"],
            [22, 24, "버스"],
            [27, 28, "안"]
        ]
        assert tokens[0] == expected_tokens

    def test_record_v2(self):
        text = "오늘 아침 학교 가는 버스안에서\n학교 가자\n지미 카터"
        nuggets = list(self.nugget.record_v2([text]))

        assert nuggets
        assert type(nuggets[0]) == CorpusRecord

    def test_record_and_filter_v2_nugget(self):
        text = "오늘 아침 학교 가는 버스안에서"

        itr_nugget = self.nugget.record_v2([text])
        res_nugget = next(itr_nugget) if itr_nugget else None

        tokens = self.nugget.filter_v2(
            nuggets=[res_nugget], white_tags=["N", "V"], result_format=NuggetFilterResultFormat.NUGGET
        )

        expected_tokens = [
            CorpusToken(start=3, end=5, pos="N", surface="아침"),
            CorpusToken(start=10, end=12, pos="N", surface="학교"),
            CorpusToken(start=20, end=21, pos="V", surface="가"),
            CorpusToken(start=22, end=24, pos="N", surface="버스"),
            CorpusToken(start=27, end=28, pos="N", surface="안")
        ]
        assert tokens[0] == expected_tokens

    def test_record_and_filter_v2(self):
        text = "오늘 아침 학교 가는 버스안에서\n학교 가자\n지미 카터"
        nuggets = list(self.nugget.record_v2([text]))
        tokens = self.nugget.filter_v2(
            nuggets, white_tags=["N", "V"], result_format=NuggetFilterResultFormat.B_E_LEX
        )

        expected_tokens = [
            CorpusTokenOnly(start=3, end=5, surface="아침"),
            CorpusTokenOnly(start=10, end=12, surface="학교"),
            CorpusTokenOnly(start=20, end=21, surface="가"),
            CorpusTokenOnly(start=22, end=24, surface="버스"),
            CorpusTokenOnly(start=27, end=28, surface="안")
        ]
        assert tokens[0] == expected_tokens

    def test_bigrams(self):
        text = "오늘 아침 학교 가는 버스안에서\n학교 가자\n지미 카터"
        white_ptags = ['V', 'N', 'J', 'M', 'SL', 'SH', 'SN']
        nuggets = list(
            self.nugget.bigrams(
                [text], white_tags=white_ptags, result_format=NuggetFilterResultFormat.B_E_LEX
            )
        )
        assert nuggets == [[('오늘', '아침'), ('아침', '학교'), ('학교', '가'), ('가', '버스'), ('버스', '안'), ('안', '에서')]]

    def test_bigrams_also_selective_tags(self):
        text = "오늘 아침 학교 가는 버스안에서\n학교 가자\n지미 카터"
        white_ptags = {
            'unigram': ['V', 'N', 'SL', 'SH', 'SN'],
            'bigram': ['V', 'N', 'J', 'M', 'SL', 'SH', 'SN'],
        }
        nugget_bigrams, nugget_unigrams = list(
            self.nugget.bigrams_also_selective_tags(
                [text], white_tags_dict=white_ptags
            )
        )
        assert nugget_unigrams == [['아침', '학교', '가', '버스', '안']]
        assert nugget_bigrams == [[('오늘', '아침'), ('아침', '학교'), ('학교', '가'), ('가', '버스'), ('버스', '안'), ('안', '에서')]]

    def test_bigrams_also_selective_tags_with_preprocess(self):
        regex_normalize_nums = re.compile('\d+')
        
        def preprocess_for_title(text):
            text = regex_normalize_nums.sub('0', text)
            text = ' '.join(map(lambda w: ''.join(filter(lambda c: c.isdigit() or is_hangul(c), w)), text.split(' ')))
            text = re.sub('  ', ' ', text)
            return text

        text = "카페 반발한 ‘일회용컵 보증금‘…환경장관 “12월 분명히 시행“" 
        text = preprocess_for_title(text)
        white_ptags = {
            'unigram': ['V', 'N', 'SL', 'SH', 'SN'],
            'bigram': ['V', 'N', 'J', 'M', 'SL', 'SH', 'SN'],
        }
        nugget_bigrams, nugget_unigrams = list(
            self.nugget.bigrams_also_selective_tags(
                [text], white_tags_dict=white_ptags
            )
        )
        assert nugget_unigrams == [['카페', '반발', '일회용', '컵', '보증금', '환경', '장관', '0', '월', '시행']]
        assert nugget_bigrams == [[('카페', '반발'), ('반발', '일회용'), ('일회용', '컵'), ('컵', '보증금'), ('보증금', '환경'), ('환경', '장관'), ('장관', '0'), ('0', '월'), ('월', '분명히'), ('분명히', '시행')]]

    def test_disappearable_texts(self):
        texts = [
            "안녕하세요",
            '담당자 :  MBTI 결과: TCI 결과: 자극추구 / 위험회피 / 인내력 / 자율성 1. 숙련도/경험 :  2. 상호작용 스킬 :  3. 여아/남아 및 연령 :  4. 자란다 활동 목적 :  5. 이 선생님이랑 가장 잘 맞을 것 같은 수업 :  6. 이 선생님이랑 가장 잘 안맞을 것 같은 수업 :  7. 시간엄수 : ',
            '⠀⠀⠀⠀⠀⠀⠀',
            '◡̈',
            ' ',
            "잘가세요"
        ]
        ptexts = [preprocess_korean(text) for text in texts]
        result = list(self.nugget.record_v2(texts))
        num_result = len(result)
        assert num_result == len(texts)
