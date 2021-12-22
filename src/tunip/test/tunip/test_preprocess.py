import unittest

from tunip.preprocess import preprocess_korean


class PreprocessTest(unittest.TestCase):

    def test_preprocess_korean(self):
        text = '∀Twitch Plays Pokémon/시즌 1/2주차'
        expected = '∀Twitch Plays Pokémon/시즌 1/2주차'

        preprocessed = preprocess_korean(text)
        print(preprocessed)
        assert preprocessed == expected
