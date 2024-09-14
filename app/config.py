import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    @staticmethod
    def get_mysql_uri():
        user = os.getenv("MYSQL_USER")
        password = os.getenv("MYSQL_PASSWORD")
        host = os.getenv("MYSQL_HOST")
        database = os.getenv("MYSQL_DATABASE")
        return f"mysql+pymysql://{user}:{password}@{host}/{database}"

    # Google Sheets settings
    GOOGLE_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID")
    GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
