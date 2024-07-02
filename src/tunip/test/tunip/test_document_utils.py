import unittest

from tunip.document_utils import DocumentRecord, AnchorDocumentRecord, ClannDocumentRecord

class DocumentUtilsTest(unittest.TestCase):

    def setUp(self):
        pass
          
    def test_document_utils(self):

        doc_id = 5
        parent_title = "지미 카터"
        parent_text = "지미 카터는 조지아주 섬터 카운티 플레인스 마을에서 태어났다."
        anchor_title = "1924년␟10월 1일␟민주당 (미국)␟미국␟1977년␟1981년␟조지아주␟조지아 공과대학교␟1953년"
        anchor_text = "1924년␟10월 1일␟민주당␟미국␟1977년␟1981년␟조지아주␟조지아 공과대학교␟1953년"
        
        doc_record = DocumentRecord(
            doc_id=doc_id,
            title=parent_title,
            text=parent_text
        )

        assert doc_record.doc_id == doc_id
        assert doc_record.title == parent_title
        assert doc_record.text == parent_text
        
        anchor_doc_record = AnchorDocumentRecord(
            doc_id=doc_id,
            parent_title=parent_title,
            parent_text=parent_text,
            anchor_title=anchor_title,
            anchor_text=anchor_text
        )
        
        assert anchor_doc_record.doc_id == doc_id
        assert anchor_doc_record.parent_title == parent_title
        assert anchor_doc_record.parent_text == parent_text
        assert anchor_doc_record.anchor_title == anchor_title
        assert anchor_doc_record.anchor_text == anchor_text


        feature = 'airbus j138 The Airbus A318 is the smallest airliner in the Airbus A320 family.'
        label = 'kc:/aviation'
        clann_doc_record = ClannDocumentRecord(
            doc_id=doc_id,
            feature=feature,
            label=label
        )

        assert clann_doc_record.doc_id == doc_id
        assert clann_doc_record.feature == feature
        assert clann_doc_record.label == label

        

if __name__=="__main__":
    unittest.main()