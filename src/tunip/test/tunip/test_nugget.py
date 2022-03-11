import unittest

from tunip.corpus_utils_v2 import CorpusToken, CorpusTokenOnly
from tunip.nugget_api import Nugget, NuggetFilterResultFormat


class NuggetTest(unittest.TestCase):
    def setUp(self):
        self.nugget = Nugget(tagger_type="seunjeon")

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
