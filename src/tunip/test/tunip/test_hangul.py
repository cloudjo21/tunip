import unittest
import json

from collections import Counter
from itertools import filterfalse, starmap

from tunip.corpus_utils import CorpusToken
from tunip.Hangulpy import has_jongsung, JONGSUNGS
from tunip.Hangulpy import decompose, compose
from tunip.Hangulpy import is_hangul_jamo_char, concat_stem_and_affix
from tunip.Hangulpy import is_agglunated, concat_agglunated_tokens


class TestHangul(unittest.TestCase):
    def setUp(self):
        pass

    def test_compose(self):
        stem, affix = "살펴보", "ㄹ"
        expected = "살펴볼"

        assert has_jongsung(stem[-1]) is False
        assert affix[0] in JONGSUNGS

        cho, joong, _ = decompose(stem[-1])
        new_stem_last_letter = compose(cho, joong, affix[0])
        assert new_stem_last_letter == "볼"

        actual = f"{stem[:-1]}{new_stem_last_letter}{affix[1:]}"
        assert actual == expected
        assert len(actual) == (len(stem) + len(affix) - 1)

    def test_compose_agglunated(self):
        stem, affix = "살펴보", "ㄹ"
        expected = "살펴볼"
        actual = concat_stem_and_affix(stem, affix)
        assert actual == expected

    def test_is_agglunative_bi_tokens(self):
        tokens = ["앞으로", "살펴보", "ㄹ", "예정", "이다"]
        bi_tokens = list(zip(tokens, tokens[1:]))
        is_agglunated_tokens = list(starmap(is_agglunated, bi_tokens))
        assert is_agglunated_tokens[1] is True
        
    def test_concat_agglunated_tokens_pass_not_hangul(self):
        tokens = ["ㅿ", "포ㅿ", "@", "㊄", "な", "и", "邀"]
        expected_indices = [[0], [1], [2], [3], [4], [5], [6]]

        new_tokens, token_indices = concat_agglunated_tokens(tokens)
        assert new_tokens == tokens
        assert token_indices == expected_indices

    def test_concat_agglunated_bi_tokens(self):
        expected = ["앞으로", "살펴볼", "예정", "이다"]
        tokens = ["앞으로", "살펴보", "ㄹ", "예정", "이다"]
        expected_indices = [[0], [1, 2], [3], [4]]

        new_tokens, token_indices = concat_agglunated_tokens(tokens)
        assert new_tokens == expected
        assert token_indices == expected_indices

    def test_concat_agglunated_bi_tokens_0(self):
        expected = ["다가올", "문제", "때문", "인", "지요"]
        tokens = ["다가올", "문제", "때문", "이", "ㄴ", "지요"]
        expected_indices = [[0], [1], [2], [3, 4], [5]]

        new_tokens, token_indices = concat_agglunated_tokens(tokens)
        assert new_tokens == expected
        assert token_indices == expected_indices


    def test_concat_agglunated_bi_tokens_1(self):
        expected = ["다가올", "문제", "때문", "인지요"]
        tokens = ["다가올", "문제", "때문", "이", "ㄴ지요"]
        expected_indices = [[0], [1], [2], [3, 4]]

        new_tokens, token_indices = concat_agglunated_tokens(tokens)
        assert new_tokens == expected
        assert token_indices == expected_indices


    def test_concat_agglunated_bi_tokens_2(self):
        
        expected = ["들어오", "아", "보", "아", "ㅈㅂ", "님", "아"]
        tokens = ["들어오", "아", "보", "아", "ㅈ", "ㅂ", "님", "아"]
        expected_indices = [[0], [1], [2], [3], [4, 5], [6], [7]]

        new_tokens, token_indices = concat_agglunated_tokens(tokens)
        assert new_tokens == expected
        assert token_indices == expected_indices

    def test_concat_agglunated_bi_tokens_3(self):
        
        expected = ['농협콕뱅크', '앱', '쓰', '는', '애', '들', '아', '들어오', '아', '보', '아', 'ㅈㅂ', ':', '네이트판']
        tokens = ['농협콕뱅크', '앱', '쓰', '는', '애', '들', '아', '들어오', '아', '보', '아', 'ㅈ', 'ㅂ', ':', '네이트판']
        expected_indices = [[0], [1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11, 12], [13], [14]]

        new_tokens, token_indices = concat_agglunated_tokens(tokens)
        assert new_tokens == expected
        assert token_indices == expected_indices


    def test_concat_agglunated_bi_tokens_4(self):
        
        expected = ['변화', '생길까']
        tokens = ['변화', '생기', 'ㄹ까']
        expected_indices = [[0], [1, 2]]

        new_tokens, token_indices = concat_agglunated_tokens(tokens)
        assert new_tokens == expected
        assert token_indices == expected_indices


    def test_concat_agglunated_bi_tokens_5(self):
        line = """{"text": "[9] 이 때문인지 동아일보 측은 본사와 신뢰와 의리를 맺어온 것에 감사하며 2003년과 2013년에 각각 감사패 ...", 
        "labels": [[1, 2, "QT_ORDER"], [43, 48, "DT_YEAR"], [50, 55, "DT_YEAR"]], 
        "tokens": [[0, 1, "SS", "["], [1, 2, "SN", "9"], [2, 3, "SS", "]"], [4, 5, "MM", "이"], [6, 8, "NNB", "때문"], [8, 9, "VCP", "이"], [8, 10, "EC", "ㄴ지"], [11, 15, "NNP", "동아일보"], [16, 17, "NNB", "측"], [17, 18, "JX", "은"], [19, 21, "NNG", "본사"], [21, 22, "JC", "와"], [23, 25, "NNG", "신뢰"], [25, 26, "JC", "와"], [27, 29, "NNG", "의리"], [29, 30, "JKO", "를"], [31, 32, "VV", "맺"], [32, 33, "EC", "어"], [33, 34, "VX", "오"], [33, 34, "ETM", "ㄴ"], [35, 36, "NNB", "것"], [36, 37, "JKB", "에"], [38, 40, "NNG", "감사"], [40, 41, "XSV", "하"], [41, 42, "EC", "며"], [43, 47, "SN", "2003"], [47, 48, "NNB", "년"], [48, 49, "JC", "과"], [50, 54, "SN", "2013"], [54, 55, "NNB", "년"], [55, 56, "JKB", "에"], [57, 59, "MAG", "각각"], [60, 62, "NNG", "감사"], [62, 63, "NNG", "패"], [64, 65, "SF", "."], [65, 66, "SF", "."], [66, 67, "SF", "."]]}"""

        expected_surfaces_concat = ['[', '9', ']', '이', '때문', '인지', '동아일보', '측', '은', '본사', '와', '신뢰', '와', '의리', '를', '맺', '어', '온', '것', '에', '감사', '하', '며', '2003', '년', '과', '2013', '년', '에', '각각', '감사', '패', '.', '.', '.']
        expected_token_indices = [[0], [1], [2], [3], [4], [5, 6], [7], [8], [9], [10], [11], [12], [13], [14], [15], [16], [17], [18, 19], [20], [21], [22], [23], [24], [25], [26], [27], [28], [29], [30], [31], [32], [33], [34], [35], [36]]
        expected_surfaces_head = ['[', '9', ']', '이', '때문', '이', '동아일보', '측', '은', '본사', '와', '신뢰', '와', '의리', '를', '맺', '어', '오', '것', '에', '감사', '하', '며', '2003', '년', '과', '2013', '년', '에', '각각', '감사', '패', '.', '.', '.']

        obj = json.loads(line)
        tokens: List[CorpusToken] = [
            CorpusToken.from_tuple_entry(t) for t in obj["tokens"]
        ]
        surfaces = [t.surface for t in tokens]

        surfaces_concat, token_indices = concat_agglunated_tokens(surfaces)
        head_token_indices = [t[0] for t in token_indices]
        tokens_updated: List[CorpusToken] = [
            tokens[token_index] for token_index in head_token_indices
        ]
        surfaces_updated = [x.surface for x in tokens_updated]
        assert surfaces_concat == expected_surfaces_concat
        assert token_indices == expected_token_indices
        assert surfaces_updated == expected_surfaces_head

    
    def test_concat_agglunated_bi_tokens_6(self):
        tokens = [[6, 8, "NNG", "송금"], [8, 9, "JKO", "을"], [10, 11, "VV", "하"], [10, 11, "EP", "었"], [11, 13, "EC", "는데"], [14, 16, "NNG", "수취"], [16, 17, "XSN", "인"]]
        tokens = [CorpusToken.from_tuple_entry(t) for t in tokens]
        surfaces = [t.surface for t in tokens]

        surfaces_updated, token_indices = concat_agglunated_tokens(surfaces)
        assert surfaces_updated == ['송금', '을', '하', '었', '는데', '수취', '인']
        assert token_indices == [[0], [1], [2], [3], [4], [5], [6]]

        assert True
    
    def test_concat_agglunated_bi_tokens_7(self):
        tokens = ['이', '밖', '에', '다른', '관점', '의', '초성', '활용형', '이', '있', '다', '.', '초성', '활용형', '은', '10', '개', '의', '숫자', '키', '에', '자음', '중심', '으로', '할당', '한다', '.', '대개', "'", 'ㄱ', 'ㄴ', 'ㄷ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅅ', 'ㅇㅈ', 'ㅎ', "'", '을', '기본', '으로', '배열', '하', '고', '다른', '자음', '은', '그', '다음', '층', '(', '레이어', ')', '로', '할당', '한다', '.', '과거', '체신', '부', '전화기', '규격집', '(', '1988', '.', '11', ')', '으로', '정해진', '배열', '을', '맥', '슨', '전자', '의', '무선', '전화', '에', '적용', '된', '예', '가', '있', '다', '.', '김민겸', '한글', '은', '초성', '활용', '형', '에', '해당', '하', '지만', 'ㅎ', '을', '모음', '용', '으로', '겸하', '고', ',', '*', ',', '#', '을', '모음', '용', '및', '자음', '변환용', '으로', '겸하', '는', '독특', '한', '설계', '이', '다', '.', '자음', '모음', '교대', '형', '은', '초성', '활용', '모드', '로', '변환', '할', '수', '있', '다', '.', '개인', '들', '이', '초성', '활용', '운동', '을', '벌리', '기', '도', '하', '는데', ',', 'ㅎ', '대신', 'ㅊ을', '기본', '으로', '배열', '하', '였', '다', '.', '그러나', '일부', '학자', '들', '은', '과거', '체신', '부', '전화기', '규격', '을', '따르', '되', ',', '다만', '0', '번', '키', '에', '심리학', '적', '인', '정합성', '을', '위해', 'ㅇ을', '배열', '해야', '하', '고', '따라서', 'ㅇ과', 'ㅎ', '의', '위치', '만', '바꾸', '어야', '한다고', '주장', '한다', '.', '초성', '10', '자', '의', '빈도', '순서', '는', '자음', '의', '특성', '에서', '보', '듯', 'ㅇㄴ', 'ㄱ', 'ㄹ', 'ㅅ', 'ㄷ', 'ㅈ', 'ㅎ', 'ㅁ', 'ㅂ', '이', '다', '.']
        is_agglunated_tokens = [False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False]

        for i, (t, ig) in enumerate(zip(tokens, is_agglunated_tokens)):
            if ig is True and i < len(tokens) - 1:
                print(t + ", " + tokens[i+1] + "[" + str(is_agglunated(t, tokens[i+1])) + "]")
            elif ig is True:
                print(t + "$")

        assert True


# agglutinated_counter = Counter(
#     {
#         "하_ㄴ": 137,
#         "하_ㄹ": 90,
#         "하_어": 80,
#         "이_ㄴ": 66,
#         "하_ㅂ니다": 55,
#         "이_ㅂ니다": 54,
#         "되_ㄴ": 48,
#         "하_었": 41,
#         "통하_어": 39,
#         "따르_아": 24,
#         "하_어야": 20,
#         "되_ㅂ니다": 19,
#         "나_의": 18,
#         "위하_ㄴ": 17,
#         "이_ㄹ": 16,
#         "대하_어": 12,
#         "대하_ㄴ": 11,
#         "드리_ㅂ니다": 11,
#         "하_어서": 10,
#         "위하_어": 10,
#         "시_ㄴ": 10,
#         "쓰_ㄹ": 10,
#         "관하_ㄴ": 9,
#         "대하_어서": 9,
#         "위하_어서": 9,
#         "것_이": 9,
#         "알리_어": 8,
#         "하_ㄴ다": 8,
#         "이_ㄴ가": 8,
#         "이_다": 8,
#         "빠르_ㄴ": 7,
#         "보_ㄴ": 7,
#         "자세하_ㄴ": 7,
#         "통하_ㄴ": 7,
#         "밝히_었": 7,
#         "아니_ㄴ": 6,
#         "통하_어서": 6,
#         "이_라는": 6,
#         "에_ㄴ": 6,
#         "지나_ㄴ": 6,
#         "하_ㅁ": 5,
#         "하_어요": 5,
#         "시_어요": 5,
#         "하_ㄴ다고": 5,
#         "오_ㄴ": 5,
#         "되_ㄴ다": 5,
#         "바라_ㅂ니다": 5,
#         "시_ㄹ": 5,
#         "되_었": 5,
#         "시_어야": 5,
#         "누르_어": 4,
#         "이_ㄹ까": 4,
#         "하_어도": 4,
#         "저_의": 4,
#         "따르_ㄴ": 4,
#         "크_ㄴ": 4,
#         "지_어": 4,
#         "늘어나_ㄹ": 4,
#         "이_라면": 4,
#         "되_ㄹ": 4,
#         "하_ㄴ데": 4,
#         "그리_어": 4,
#         "내_ㄴ": 4,
#         "쓰_어": 4,
#         "더하_ㅂ니다": 4,
#         "이_ㄴ지": 3,
#         "오_았": 3,
#         "주_ㄴ다": 3,
#         "보_ㅂ니다": 3,
#         "나오_ㄴ": 3,
#         "지_었": 3,
#         "되_ㄹ까": 3,
#         "이_라고": 3,
#         "똑똑하_ㄴ": 3,
#         "아_보": 3,
#         "인하_어": 3,
#         "드리_ㄹ": 3,
#         "이_ㄴ데": 3,
#         "정하_어": 3,
#         "쓰_었": 3,
#         "모으_아": 3,
#         "이_고": 3,
#         "하_었었": 3,
#         "하_ㄴ가": 3,
#         "떠나_아": 3,
#         "나오_ㄴ다": 3,
#         "빠지_어요": 3,
#         "펑키하_ㄴ": 3,
#         "옮기_ㄴ": 3,
#         "편리하_ㄴ": 3,
#         "복잡하_ㄴ": 3,
#         "알아보_ㄹ까": 3,
#         "알_ㄴ다면": 2,
#         "기_ㄴ": 2,
#         "인하_ㄴ": 2,
#         "이_냐면": 2,
#         "시_었": 2,
#         "것_이_이_ㅂ니다": 2,
#         "하_ㄹ까": 2,
#         "되_ㅁ": 2,
#         "열_ㄴ": 2,
#         "이_었": 2,
#         "내려가_ㄴ": 2,
#         "보_ㄴ다면": 2,
#         "으시_ㄹ": 2,
#         "간단하_ㄴ": 2,
#         "때_ㄴ": 2,
#         "의하_어": 2,
#         "보이_어요": 2,
#         "지_ㄴ": 2,
#         "편하_ㄴ": 2,
#         "나_아서": 2,
#         "생기_ㄴ": 2,
#         "널알리_어": 2,
#         "주_어": 2,
#         "보이_어": 2,
#         "선보이_ㄴ다고": 2,
#         "기_ㄹ": 2,
#         "이_요": 2,
#         "맡기_어도": 2,
#         "시_ㄴ다면": 2,
#         "시_어서": 2,
#         "가_ㄹ": 2,
#         "하_어서_어서_페이커": 2,
#         "크_었": 2,
#         "알아보_ㄹ게": 2,
#         "숨기_어": 2,
#         "뒤지_어야": 2,
#         "기등록되_ㄴ": 2,
#         "지_ㄴ다": 2,
#         "으시_ㄴ": 2,
#         "살펴보_ㄹ": 2,
#         "보_ㄹ까": 2,
#         "태어나_ㄴ다": 2,
#         "바꾸_ㄴ": 2,
#         "하_ㄴ다는": 2,
#         "그것_으로": 2,
#         "나_ㄴ": 2,
#         "이_라": 2,
#         "맡기_ㄴ": 2,
#         "타_아": 2,
#         "저렴하_ㄴ": 2,
#         "나오_았": 2,
#         "넉넉하_ㄴ": 2,
#         "달라지_ㄹ": 2,
#         "낮아지_ㄴ다": 2,
#         "엄연하_ㄴ": 1,
#         "올리_어": 1,
#         "다르_ㄴ데": 1,
#         "생기_었": 1,
#         "막히_었": 1,
#         "드리_었": 1,
#         "미포하_ㅁ": 1,
#         "이것_이": 1,
#         "보내_ㅂ니다": 1,
#         "찌_ㄴ": 1,
#         "두드리_ㅁ": 1,
#         "앞서_ㄴ": 1,
#         "익숙하_ㄴ": 1,
#         "살리_ㅂ니다": 1,
#         "바꾸_ㄹ": 1,
#         "알아보_ㅂ시다": 1,
#         "생기_ㄹ": 1,
#         "건드리_어": 1,
#         "어야_하": 1,
#         "시키_어야": 1,
#         "이것_ㄹ": 1,
#         "너_의": 1,
#         "어_드리_드리_ㅂ니다": 1,
#         "계시_어요": 1,
#         "나_에게": 1,
#         "계시_ㄴ": 1,
#         "접하_어": 1,
#         "쌓이_어": 1,
#         "아끼_어야": 1,
#         "으시_었": 1,
#         "달하_었": 1,
#         "걸리_어": 1,
#         "바꾸_었": 1,
#         "흐르_었": 1,
#         "못하_었": 1,
#         "넘기_었": 1,
#         "더디_ㄹ": 1,
#         "고급스렇_ㄴ": 1,
#         "야무지_ㄴ": 1,
#         "어디_이_이_ㄴ지": 1,
#         "보_았": 1,
#         "이_야": 1,
#         "줄서_ㄴ다": 1,
#         "팔리_었": 1,
#         "지나_아": 1,
#         "간편하_ㄴ": 1,
#         "알아보_ㅂ니다": 1,
#         "살펴보_ㄹ까": 1,
#         "되_ㄴ다더라구": 1,
#         "웬만하_ㄴ": 1,
#         "궁금하_어": 1,
#         "만들_ㄹ": 1,
#         "만기하_어": 1,
#         "잇달_ㄴ": 1,
#         "두_ㄹ": 1,
#         "잇따르_아": 1,
#         "답답하_ㄴ": 1,
#         "보내_ㄹ": 1,
#         "주_ㄹ": 1,
#         "채워야하_ㅁ": 1,
#         "내놓_았": 1,
#         "그냥하_어": 1,
#         "계시_ㅂ니다": 1,
#         "주_ㄹ지": 1,
#         "주어지_ㄴ": 1,
#         "채우_ㄴ다면": 1,
#         "돌려주_ㄴ다": 1,
#         "ㅣ신하_ㄴ": 1,
#         "나오_ㅂ니다": 1,
#         "계시_었": 1,
#         "알리_ㅁ": 1,
#         "낮아지_어서": 1,
#         "들어오_ㄴ": 1,
#         "보이_었": 1,
#         "늘_ㄹ수록": 1,
#         "많아지_ㄴ다": 1,
#         "오르_ㄴ다는": 1,
#         "나아지_ㄹ": 1,
#         "보_ㄹ": 1,
#         "돌리_어야": 1,
#         "하_ㄴ다면": 1,
#         "되_어요": 1,
#         "터_이": 1,
#         "커지_ㄴ다": 1,
#         "권하_ㄴ다": 1,
#         "단,하_어": 1,
#         "살피_어": 1,
#         "뜨_어서": 1,
#         "피하_ㄹ": 1,
#         "당하_었": 1,
#         "나가_아": 1,
#         "들이_어": 1,
#         "비슷하_어요": 1,
#         "다하_어서": 1,
#         "달_ㄴ다": 1,
#         "버리_었": 1,
#         "드리_ㄹ게": 1,
#         "까하_어요": 1,
#         "약하_어요": 1,
#         "만들_ㅂ니다": 1,
#         "주_ㄴ": 1,
#         "주_ㄴ다고": 1,
#         "신규하_ㄴ": 1,
#         "이루_었": 1,
#         "바꾸_ㄴ다": 1,
#         "만나_ㄹ": 1,
#         "이루_ㄴ다": 1,
#         "가_아": 1,
#         "명확하_ㄴ": 1,
#         "보내_ㅂ시다": 1,
#         "넘어서_었": 1,
#         "로되_ㄴ": 1,
#         "나타내_ㄴ": 1,
#         "빌리_어": 1,
#         "살_ㄹ": 1,
#         "따르_ㅁ": 1,
#         "건너_어": 1,
#         "이_네": 1,
#         "하_어도_어도_.": 1,
#         "낮아지_ㅂ니다": 1,
#         "줄어들_ㅁ": 1,
#         "넘어오_ㅁ": 1,
#         "되_어": 1,
#         "이루어지_ㄴ": 1,
#         "건실하_ㄴ": 1,
#         "은행되_ㄹ": 1,
#         "하_어도_어도_한화생명": 1,
#         "오_아": 1,
#         "알아보_아야": 1,
#         "바쁘_ㄴ": 1,
#         "높아지_ㄹ수록": 1,
#         "뜨_ㄴ다": 1,
#         "내_ㄹ": 1,
#         "만하_ㄴ": 1,
#         "놀라_ㄴ": 1,
#         "올리_었": 1,
#         "힘들_ㄹ": 1,
#         "드리_어": 1,
#         "죄송하_ㅂ니다": 1,
#         "한하_어": 1,
#         "이르_ㄴ다": 1,
#         "핫하_ㄴ": 1,
#         "오르_았": 1,
#         "뜨_었": 1,
#         "이_에요": 1,
#         "쓰_ㄹ수록": 1,
#         "갈아타_ㄴ": 1,
#         "떨어지_ㄴ": 1,
#         "수_ㄴ": 1,
#         "낯설_ㄴ": 1,
#         "아_만": 1,
#         "막히_ㄴ": 1,
#         "적히_어": 1,
#         "이_ㄹ지": 1,
#         "기하_어서": 1,
#         "하_어야만": 1,
#         "이끌_ㄹ": 1,
#         "거치_어": 1,
#         "열리_ㄴ": 1,
#         "그렇_ㄴ지": 1,
#         "하_어서_어서_.": 1,
#         "예쁘_ㄴ": 1,
#         "리뷰하_ㄹ": 1,
#         "가_았": 1,
#         "선보이_었": 1,
#         "무관하_ㅂ니다": 1,
#         "넘기_ㄹ": 1,
#         "어떻_ㄴ가": 1,
#         "이르_ㄴ": 1,
#         "상이하_ㅁ": 1,
#         "들어오_았": 1,
#         "이_ㅁ": 1,
#         "올바르_ㄴ": 1,
#         "빌리_어서": 1,
#         "나_았": 1,
#         "빌리_ㄹ": 1,
#         "의하_ㄴ": 1,
#         "빌리_ㄴ": 1,
#         "정하_ㄴ": 1,
#         "즐기_어": 1,
#     }
# )

# print("Morph particles that are more than 2")
# for key, val in agglutinated_counter.items():
#     splits = key.split("_")
#     if len(splits) > 2:
#         print(splits)
# print("#END-OF Morph particles that are more than 2")