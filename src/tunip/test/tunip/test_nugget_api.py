import unittest
from tunip.nugget_api import Nugget


class NuggetApiTest(unittest.TestCase):
    
    def setUp(self):
        self.nugget = Nugget()

    def test_strip_spaces(self):
        
        # 1. 유니코드 앞뒤 공백 제거 확인
        text = "1. 육군인쇄창 부지 ꡒ 병풍아파트 ꡓ 의혹"
        tokens = [[0, 1, 'SN', '1'], [1, 2, 'S', '.'], [3, 5, 'N', '육군'], [5, 6, 'VCP', '인'], 
                  [6, 8, 'N', '쇄창'], [9, 11, 'N', '부지'], [11, 14, 'S', ' ꡒ '], [14, 16, 'N', '병풍'], 
                  [16, 19, 'N', '아파트'], [19, 22, 'S', ' ꡓ '], [22, 24, 'N', '의혹']]

        expect = [[0, 1, 'SN', '1'], [1, 2, 'S', '.'], [3, 5, 'N', '육군'], [5, 6, 'VCP', '인'], 
                  [6, 8, 'N', '쇄창'], [9, 11, 'N', '부지'], [12, 13, 'S', 'ꡒ'], [14, 16, 'N', '병풍'],
                  [16, 19, 'N', '아파트'], [20, 21, 'S', 'ꡓ'], [22, 24, 'N', '의혹']]
        
        updated = self.nugget.strip_spaces(tokens)
        assert updated == expect
        
        entries = self.nugget([text])
        for entry in entries:
            assert entry["tokens"] == expect

        # 2. 연속 공백 토큰 탈락 확인
        text = "머스크는 2013 년 소더비 경매에서 촬영한 웨트 넬리 (Wet Nellie)를 샀다.  머스크는 발표 날짜에 대한 질문에 대해 7 월 말에 발표했다."
        expect = [[0, 3, 'N', '머스크'], [3, 4, 'J', '는'], [5, 9, 'SN', '2013'], [10, 11, 'N', '년'], [12, 15, 'N', '소더비'], 
                  [16, 18, 'N', '경매'], [18, 20, 'J', '에서'], [21, 23, 'N', '촬영'], [23, 24, 'XS', '한'], [25, 27, 'N', '웨트'], 
                  [28, 30, 'N', '넬리'], [31, 32, 'S', '('], [32, 35, 'SL', 'Wet'], [36, 42, 'SL', 'Nellie'], [42, 43, 'S', ')'], 
                  [43, 44, 'J', '를'], [45, 46, 'V', '샀'], [46, 47, 'E', '다'], [47, 48, 'S', '.'], [50, 53, 'N', '머스크'], 
                  [53, 54, 'J', '는'], [55, 57, 'N', '발표'], [58, 60, 'N', '날짜'], [60, 61, 'J', '에'], [62, 64, 'V', '대한'], 
                  [65, 67, 'N', '질문'], [67, 68, 'J', '에'], [69, 71, 'V', '대해'], [72, 73, 'SN', '7'], [74, 75, 'N', '월'], 
                  [76, 77, 'N', '말'], [77, 78, 'J', '에'], [79, 81, 'N', '발표'], [81, 82, 'XS', '했'], [82, 83, 'E', '다'], 
                  [83, 84, 'S', '.']]
        
        entries = self.nugget([text])
        for entry in entries:
            assert self.nugget.strip_spaces(entry["tokens"]) == expect
            
if __name__=="__main__":
    unittest.main()