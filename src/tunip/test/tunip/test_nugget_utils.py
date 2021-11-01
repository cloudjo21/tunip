import unittest
from pathlib import Path

import tunip.nugget_utils as nu
from tunip.config import Config


class NuggetUtilsTest(unittest.TestCase):

    def test_strip_spaces(self):
        # 1. 육군인쇄창 부지 ꡒ 병풍아파트 ꡓ 의혹
        tokens = [[0, 1, 'SN', '1'], [1, 2, 'S', '.'], [3, 5, 'N', '육군'], [5, 6, 'VCP', '인'], 
                  [6, 8, 'N', '쇄창'], [9, 11, 'N', '부지'], [11, 14, 'S', ' ꡒ '], [14, 16, 'N', '병풍'], 
                  [16, 19, 'N', '아파트'], [19, 22, 'S', ' ꡓ '], [22, 24, 'N', '의혹']]

        expect = [[0, 1, 'SN', '1'], [1, 2, 'S', '.'], [3, 5, 'N', '육군'], [5, 6, 'VCP', '인'], 
                  [6, 8, 'N', '쇄창'], [9, 11, 'N', '부지'], [12, 13, 'S', 'ꡒ'], [14, 16, 'N', '병풍'],
                  [16, 19, 'N', '아파트'], [20, 21, 'S', 'ꡓ'], [22, 24, 'N', '의혹']]
        
        updated = nu.strip_spaces(tokens)
        assert updated == expect