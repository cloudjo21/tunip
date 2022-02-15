import unittest

from tunip.corpus_utils import CorpusRecord


class RemoveWordsTest(unittest.TestCase):

    def test_from_json_entry_corpus_record(self):
        record_json = {"labels": [[9, 14, "QT_MAN_COUNT"]], "text": "올원뱅크 가입자 500만명…서비스 '쑥쑥' - 농민신문", "tokens": [[0, 1, "M", "올"], [1, 2, "N", "원"], [2, 4, "N", "뱅크"], [5, 8, "N", "가입자"], [9, 12, "SN", "500"], [12, 13, "N", "만"], [13, 14, "N", "명"], [14, 15, "S", "…"], [15, 18, "N", "서비스"], [19, 20, "S", "'"], [20, 22, "M", "쑥쑥"], [22, 23, "S", "'"], [24, 25, "S", "-"], [26, 28, "N", "농민"], [28, 30, "N", "신문"]]}
        record = CorpusRecord.from_json_entry(record_json)
        assert record is not None

    def test_remove_word_of_corpus_record(self):
        record_json = {"labels": [[9, 14, "QT_MAN_COUNT"]], "text": "올원뱅크 가입자 500만명…서비스 '쑥쑥' - 농민신문", "tokens": [[0, 1, "M", "올"], [1, 2, "N", "원"], [2, 4, "N", "뱅크"], [5, 8, "N", "가입자"], [9, 12, "SN", "500"], [12, 13, "N", "만"], [13, 14, "N", "명"], [14, 15, "S", "…"], [15, 18, "N", "서비스"], [19, 20, "S", "'"], [20, 22, "M", "쑥쑥"], [22, 23, "S", "'"], [24, 25, "S", "-"], [26, 28, "N", "농민"], [28, 30, "N", "신문"]]}
        record = CorpusRecord.from_json_entry(record_json)

        remove_words = ["올", "원"]
        remove_tokens = []
        remove_accum_offset = 0
        prev_end = -1
        for token in record.tokens:
            if token.surface in remove_words:
                remove_tokens.append(token)
                remove_accum_offset += len(token.surface)
                if token.start > prev_end:
                    remove_accum_offset += 1
            prev_end = token.end

        assert record is not None
