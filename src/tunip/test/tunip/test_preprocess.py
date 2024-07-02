import unittest

from tunip.preprocess import preprocess_korean, preprocess_tokens


class PreprocessTest(unittest.TestCase):

    def test_preprocess_korean(self):
        text = '∀Twitch Plays Pokémon/시즌 1/2주차'
        expected = '∀Twitch Plays Pokémon/시즌 1/2주차'

        preprocessed = preprocess_korean(text)
        print(preprocessed)
        assert preprocessed == expected

    def test_preprocess_tokens(self):
        from tunip.nugget_api import Nugget

        nugget = Nugget()
        entries = nugget("안녕하세요 트리니다드토바고에 오신 것을 환영합니다!!")
        entries = list(entries)
        preprocessed = preprocess_tokens(nugget_entries=entries, white_tags=["N"])

        assert len(preprocessed[0]) == 4
        assert preprocessed[0][0][2] == 'N'
        assert preprocessed[0][1][2] == 'N'
        assert preprocessed[0][2][2] == 'N'
        assert preprocessed[0][3][2] == 'N'

        assert preprocessed[0][1][0] == 4
        assert preprocessed[0][1][1] == 12

        assert preprocessed[0][2][0] == 13
        assert preprocessed[0][2][1] == 14

        assert preprocessed[0][3][0] == 15
        assert preprocessed[0][3][1] == 17
