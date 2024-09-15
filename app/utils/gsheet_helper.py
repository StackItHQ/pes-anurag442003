import gspread
from google.oauth2.service_account import Credentials

class GoogleSheetHelper:
    def __init__(self, credentials_path, spreadsheet_id):
        self.client = gspread.service_account(filename=credentials_path)
        self.sheet = self.client.open_by_key(spreadsheet_id).sheet1

    def get_all_records(self):
        """Fetch all records from the Google Sheet."""
        return self.sheet.get_all_records()

    def insert_row(self, data, index=1):
        """Insert a row in the Google Sheet."""
        self.sheet.insert_row(data, index)

    def update_cell(self, row, col, value):
        """Update a specific cell in the Google Sheet."""
        self.sheet.update_cell(row, col, value)

    def find_row_by_value(self, column_name, value):
        records = self.get_all_records()
        for idx, record in enumerate(records):
            if record[column_name] == value:
                return idx + 2  # because Sheets rows are 1-indexed and header row is first

    def delete_row(self, index):
        """Delete a specific row from the Google Sheet."""
        self.sheet.delete_row(index)

    def clear_sheet(self):
        self.sheet.clear()
