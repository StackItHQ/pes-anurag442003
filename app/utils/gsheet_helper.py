import gspread
from google.oauth2.service_account import Credentials

class GoogleSheetHelper:
    def __init__(self, credentials_path, spreadsheet_id):
        self.client = gspread.service_account(filename=credentials_path)
        self.sheet = self.client.open_by_key(spreadsheet_id).sheet1

    def _get_gsheet_client(self):
        """Authenticate using the service account and return a Google Sheets client."""
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(self.credentials_file, scopes=scopes)
        return gspread.authorize(creds)

    def get_all_records(self):
        """Fetch all records from the Google Sheet."""
        return self.sheet.get_all_records()

    def insert_row(self, data, index=1):
        """Insert a row in the Google Sheet."""
        self.sheet.insert_row(data, index)

    def update_cell(self, row, col, value):
        """Update a specific cell in the Google Sheet."""
        self.sheet.update_cell(row, col, value)

    def delete_row(self, index):
        """Delete a specific row from the Google Sheet."""
        self.sheet.delete_row(index)
