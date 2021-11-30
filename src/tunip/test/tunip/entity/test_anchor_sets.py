import unittest

from tunip.entity.anchors import (
    Anchor,
    AnchorInstance
)
from tunip.entity.anchor_sets import AnchorSet, AnchorInstanceSet

class AnchorSetsTest(unittest.TestCase):

    def test_init_anchors_set(self):
        
        id = 0
        anchor_title="미국"
        anchor_first_sent="미합중국(United States of America, 약칭: USA), 약칭 합중국(United States, 약칭: U.S.) 또는 미국은 주 50개와 특별구 1개로 이루어진 연방제 공화국이다."
        anchor_parent_title="지미 카터"
        anchor_parent_first_sent="제임스 얼 카터 주니어 ( James Earl Carter , Jr . , 1924 년 10 월 1 일 ~ ) 는 민주당 출신 미국 39 대 대통령 ( 1977 년 ~ 1981 년 ) 이 다 ."
        
        anchor = Anchor(id, anchor_title, anchor_first_sent, anchor_parent_title, anchor_parent_first_sent)
        assert anchor.anchor_title == "미국"
        
        anchor_set = AnchorSet(anchors=[anchor])
        
        assert anchor_set.anchors[0].id == 0
        assert anchor_set.anchors[0].anchor_title == "미국"
        assert anchor_set.anchors[0].anchor_parent_title == "지미 카터"
        
    def test_init_anchor_instances_set(self):
        
        id=0
        anchor_text="미국"
        context_token="['민주당', '출신', '[MASK]', '39', '대']"
        
        anchor_instance = AnchorInstance(id, anchor_text, context_token)
        assert anchor_instance.anchor_text == "미국"
        
        anchor_instance_set = AnchorInstanceSet(instances=[anchor_instance])

        assert anchor_instance_set.instances[0].id == 0
        assert anchor_instance_set.instances[0].anchor_text == "미국"
        assert anchor_instance_set.instances[0].context_token == "['민주당', '출신', '[MASK]', '39', '대']"