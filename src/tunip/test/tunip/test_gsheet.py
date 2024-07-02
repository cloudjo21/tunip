import unittest

from pandas import DataFrame
from pathlib import Path

from tunip import GSheet
from tunip.env import NAUTS_HOME
from tunip.service_config import get_service_config


class GSheetTest(unittest.TestCase):

    def setUp(self):
        service_config = get_service_config()
        service_file = str(Path(NAUTS_HOME) / 'resources' / f"{service_config.config.get('gcs.project_id')}.json")
        team_drive_id='1r6gwmSyVJ8omxnuJaA5o_P8jYyjL-76vpjqjeI-P_WY'
        self.client = GSheet(service_file=service_file, team_drive_id=team_drive_id)

    def test_create_sheet(self):
        title = "ForTestSpreadSheet"
        folder = "keywords"
        sheet = self.client.create_sheet(title=title, folder_name=folder)

        assert "docs.google.com/spreadsheets" in sheet.url
        assert title == sheet.title

        wks_name = 'temp_work_sheet'
        sheet.add_worksheet(wks_name)
        assert wks_name in [wks.title for wks in sheet.worksheets()]

    def test_write_df(self):
        title = "paani_setting"
        df = DataFrame([[1, 2, 3], [0.1, 0.2, 0.3], [-0.1, -0.2, -0.3]], columns=["안녕", "하세", "요"])

        sheet = self.client.get_sheet(title)
        wks = sheet.sheet1
        wks.set_dataframe(df, start=(0, 0))
        cells = wks.get_all_values()

        assert float(cells[1][1]) == float(df.iloc[0][1])
        assert float(cells[2][2]) == float(df.iloc[1][2])

    def test_get_sheet_by_title(self):
        sheet_url = 'https://docs.google.com/spreadsheets/d/1r6gwmSyVJ8omxnuJaA5o_P8jYyjL-76vpjqjeI-P_WY/edit#gid=0'

        sheet = self.client.get_sheet(sheet_url)

        wsheet1 = sheet.worksheet_by_title('불용키워드')
        drop_tokens_df = wsheet1.get_as_df(has_header=False, index_column=0)
        drop_tokens = drop_tokens_df.values.flatten().tolist()
        assert len(drop_tokens) > 0
