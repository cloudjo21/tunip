import unittest

from tunip.document_utils import DocumentRecord, AnchorDocumentRecord

class DocumentUtilsTest(unittest.TestCase):

    def setUp(self):
        pass
          
    def test_document_utils(self):

        parent_title = "지미 카터"
        parent_text = "지미 카터는 조지아주 섬터 카운티 플레인스 마을에서 태어났다."
        anchor_title = "1924년␟10월 1일␟민주당 (미국)␟미국␟1977년␟1981년␟조지아주␟조지아 공과대학교␟1953년"
        anchor_text = "1924년␟10월 1일␟민주당␟미국␟1977년␟1981년␟조지아주␟조지아 공과대학교␟1953년"
        
        doc_record = DocumentRecord(
            title=parent_title,
            text=parent_text
        )
        
        assert doc_record.title == parent_title
        assert doc_record.text == parent_text
        
        anchor_doc_record = AnchorDocumentRecord(
            parent_title=parent_title,
            parent_text=parent_text,
            anchor_title=anchor_title,
            anchor_text=anchor_text
        )
        
        assert anchor_doc_record.parent_title == parent_title
        assert anchor_doc_record.parent_text == parent_text
        assert anchor_doc_record.anchor_title == anchor_title
        assert anchor_doc_record.anchor_text == anchor_text
        

if __name__=="__main__":
    unittest.main()