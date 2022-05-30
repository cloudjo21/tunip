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
