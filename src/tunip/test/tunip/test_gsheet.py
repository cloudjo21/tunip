import unittest

from pandas import DataFrame

from tunip import GSheet


class GSheetTest(unittest.TestCase):

    def setUp(self):
        service_file = "../resources/.secret/stable-dogfish-350507-4222975ac549.json"
        team_drive_id = "0AKyfdUW-mWZAUk9PVA"  # ai-team drive
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
        title = "ForTestSpreadSheet"
        df = DataFrame([[1, 2, 3], [0.1, 0.2, 0.3], [-0.1, -0.2, -0.3]], columns=["안녕", "하세", "요"])

        sheet = self.client.get_sheet(title)
        wks = sheet.sheet1
        wks.set_dataframe(df, start=(0, 0))
        cells = wks.get_all_values()

        assert float(cells[1][1]) == float(df.iloc[0][1])
        assert float(cells[2][2]) == float(df.iloc[1][2])
