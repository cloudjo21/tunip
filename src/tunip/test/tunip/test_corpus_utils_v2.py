import unittest
import pydantic

from tunip.corpus_utils_v2 import CorpusInput, old_nugget_return_to_v2
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
        print(new_inputs)
        assert new_inputs
