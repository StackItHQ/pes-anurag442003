from fastapi import FastAPI, Depends, HTTPException
from app.utils.gsheet_helper import GoogleSheetHelper
from app.utils.db_helper import get_db_session, DataModel, init_db
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from pydantic import BaseModel
from typing import List
import logging

logging.basicConfig(level=logging.INFO)

app = FastAPI()


@app.on_event("startup")
async def on_startup():
    await init_db()

class DataSchema(BaseModel):
    field_1: str
    field_2: str

GOOGLE_SHEET_CREDENTIALS = "C:/Users/anura/OneDrive/Desktop/GenAI/superjoin/anuragccproject-87dbca3b1d42.json"  # Ensure this path is correct
# SHEET_NAME = "Sheet1"
SPREADSHEET_ID = "1SdPrz1PTzyl-GcnaSlx9iOFml2UeUGVDWNuq41vnSfs"
gsheet_helper = GoogleSheetHelper(GOOGLE_SHEET_CREDENTIALS, SPREADSHEET_ID)

@app.get("/")
async def index():
    return "it works!"

@app.get("/sync/google-to-db")
async def sync_google_to_db(db: AsyncSession = Depends(get_db_session)):
    try:
        # Fetch all records from Google Sheets
        records = gsheet_helper.get_all_records()

        if not records:
            return {"message": "No data found in Google Sheets to sync"}

        for record in records:
            # Check if the record already exists in the database
            query = select(DataModel).filter_by(field_1=record["field_1"])
            result = await db.execute(query)
            existing = result.scalar_one_or_none()

            # Insert or update the record in the database
            if existing:
                # Update the record if it exists
                existing.field_2 = record["field_2"]
                db.add(existing)
            else:
                # Insert the new record
                new_data = DataModel(field_1=record["field_1"], field_2=record["field_2"])
                db.add(new_data)

        await db.commit()
        return {"message": "Google Sheets data synchronized with the database"}
    
    except Exception as e:
        logging.error(f"Error syncing Google Sheets to DB: {e}", exc_info=True)
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error syncing Google Sheets to DB: {str(e)}")

@app.get("/sync/db-to-google")
async def sync_db_to_google(db: AsyncSession = Depends(get_db_session)):
    try:
        # Fetch all records from the database
        result = await db.execute(select(DataModel))
        db_records = result.scalars().all()

        if not db_records:
            return {"message": "No data found in the database to sync"}

        # Sync each database record to Google Sheets
        for record in db_records:
            gsheet_helper.insert_row([record.field_1, record.field_2])

        return {"message": "Database data synchronized with Google Sheets"}

    except Exception as e:
        logging.error(f"Error syncing DB to Google Sheets: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error syncing DB to Google Sheets: {str(e)}")