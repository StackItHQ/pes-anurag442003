from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = 'C:/Users/anura/OneDrive/Desktop/sj.ai/credentials.json'  

def get_google_sheets_service():
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        return build('sheets', 'v4', credentials=creds)
    except Exception as e:
        logging.error(f"Error creating Google Sheets service: {str(e)}")
        raise

def read_sheet(sheet_id, range_name):
    service = get_google_sheets_service()
    try:
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id, range=range_name).execute()
        return result.get('values', [])
    except HttpError as error:
        logging.error(f"An error occurred while reading the sheet: {error}")
        return None

def write_sheet(sheet_id, range_name, values):
    service = get_google_sheets_service()
    try:
        body = {'values': values}
        result = service.spreadsheets().values().update(
            spreadsheetId=sheet_id, range=range_name,
            valueInputOption='USER_ENTERED', body=body).execute()
        logging.info(f"{result.get('updatedCells')} cells updated.")
        return result
    except HttpError as error:
        logging.error(f"An error occurred while writing to the sheet: {error}")
        return None

def get_sheet_structure(sheet_id):
    service = get_google_sheets_service()
    try:
        sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')
        sheet_structure = {}
        for sheet in sheets:
            title = sheet.get("properties", {}).get("title", "Sheet1")
            sheet_structure[title] = sheet.get("properties", {}).get("gridProperties", {}).get("columnCount", 0)
        return sheet_structure
    except HttpError as error:
        logging.error(f"An error occurred while getting sheet structure: {error}")
        return None

if __name__ == '__main__':
   
    SHEET_ID = '12NMmCimnsY7hzB7fBWgRHqCxbINAaAvG8Tgnsf262Uk' 
    RANGE_NAME = 'Sheet1!A1:C10'

    print(get_sheet_structure(SHEET_ID))
    print(read_sheet(SHEET_ID, RANGE_NAME))
    write_sheet(SHEET_ID, 'Sheet1!A11:C12', [['Test', 'Data', 'Here']])