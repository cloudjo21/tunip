import pandas as pd
import pygsheets
from pygsheets import SpreadsheetNotFound
from pygsheets.client import Client


class GSheet:
    """
    Args:
        service_file (str): google api credential json file
        team_drive_id (str): if want share sheets to team drive
    Note:
        pygsheets를 사용하기 위해 아래와 같은 절차가 필요합니다.
        1. 인증을 위한 credential 파일 만들기 (참조: https://erikrood.com/Posts/py_gsheets.html)
          OR we should register 'client_email' in config.get('gcs.project_id')}.json(service_account_file) as the shared user to google sheet
        2. google api console 접속 이후 [ Google Sheets API, Google Drive API ] 두가지 권한 Enable
    """
    client: Client

    def __init__(self, service_file, team_drive_id=None):
        self.client = pygsheets.authorize(service_file=service_file)
        if team_drive_id:
            self.client.drive.enable_team_drive(team_drive_id)

    def create_sheet(self, title, folder_name):
        res = self.client.create(title=title, folder_name=folder_name)
        return self.client.open_by_key(res.id)

    def get_sheet(self, name_or_url, create_if_not_exist=True):
        try:
            if name_or_url.startswith('http://') or name_or_url.startswith('https://'):
                return self.client.open_by_url(name_or_url)
            return self.client.open(name_or_url)
        except SpreadsheetNotFound:
            return self.create_sheet(name_or_url, "keywords")

    @staticmethod
    def share_with(sheet, email=None, role="writer", tpe="user"):
        if email:
            sheet.share(email, role=role, type=tpe)
        else:
            sheet.share('', role=role, type='anyone')


class WorksheetWriter:
    def __init__(self, sheet_url:str, wsheet_name: str, service_filepath: str, column_widths: list):
        self.sheet_url = sheet_url
        self.wsheet_name = wsheet_name
        self.service_filepath = service_filepath
        self.column_widths = column_widths
    
    def write(self, sheet_df: pd.DataFrame):
        gsheet = GSheet(service_file=self.service_filepath)
        sheet: pygsheets.spreadsheet.Spreadsheet = gsheet.get_sheet(self.sheet_url)
        sheet.add_worksheet(self.wsheet_name)
        worksheet = sheet.worksheet_by_title(self.wsheet_name)
        worksheet.set_dataframe(sheet_df, start=(0, 0), fit=True)
        for i, width in enumerate(self.column_widths, 1):
            worksheet.adjust_column_width(start=i, end=i, pixel_size=width)


class WorksheetReader:
    def __init__(self, sheet_url: str, wsheet_name: str, service_filepath: str):
        self.sheet_url = sheet_url
        self.wsheet_name = wsheet_name
        self.service_filepath = service_filepath
    
    def read(self):
        gsheet = GSheet(service_file=self.service_filepath)
        sheet = gsheet.get_sheet(self.sheet_url)
        wsheet = sheet.worksheet_by_title(self.wsheet_name)
        return wsheet.get_as_df()
