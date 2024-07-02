import unittest
import pydantic

from tunip.corpus_utils_v2 import (
    AdjTagCorpusTokenMerger,
    CorpusInput,
    CorpusRecord,
    CorpusToken,
    merge_surface,
    old_nugget_return_to_v2
)
from tunip.nugget_api import Nugget


class CorpusUtilsV2Test(unittest.TestCase):

    def setUp(self):
        self.nugget = Nugget()

    def test_raise_exception_when_parse_corpus_input_after_nugget(self):
        texts = ['김철수는 오늘도 학교를 간다.']
        corpus_inputs = list(self.nugget(texts))
        with self.assertRaises(pydantic.error_wrappers.ValidationError):
            ci_obj = CorpusInput.parse_obj(corpus_inputs[0])

    def test_get_v2_inputs_after_nugget(self):
        texts = ['김철수는 오늘도 학교를 간다.']
        corpus_inputs = list(self.nugget(texts))
        new_inputs = [old_nugget_return_to_v2(c) for c in corpus_inputs]
        assert new_inputs
        assert type(new_inputs[0]) == CorpusInput

    def test_post_merge_by_adjacent_poses(self):
        texts = ['매 2달마다 44.88% 금리 적용 상품']

        allow_head_pos = ['SN']
        allow_pos_for_token_merge = ['SN', 'S']
        merged_pos = 'SN'

        token_merger = AdjTagCorpusTokenMerger(allow_head_pos, allow_pos_for_token_merge, merged_pos)

        corpus_records = list(self.nugget.record_v2(texts))

        new_record = token_merger(corpus_records[0])
        assert new_record.tokens[4].surface == '44.88%'

