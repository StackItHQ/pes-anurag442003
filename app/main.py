from fastapi import FastAPI, Depends, HTTPException
from app.utils.gsheet_helper import GoogleSheetHelper
from app.utils.db_helper import get_db_session, DataModel, init_db
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from pydantic import BaseModel
from typing import List
import logging
from sqlalchemy import func

logging.basicConfig(level=logging.INFO)

app = FastAPI()

@app.on_event("startup")
async def on_startup():
    await init_db()

class DataSchema(BaseModel):
    field_1: str
    field_2: str

GOOGLE_SHEET_CREDENTIALS = "C:/Users/anura/OneDrive/Desktop/GenAI/superjoin/anuragccproject-87dbca3b1d42.json"
SPREADSHEET_ID = "1SdPrz1PTzyl-GcnaSlx9iOFml2UeUGVDWNuq41vnSfs"
gsheet_helper = GoogleSheetHelper(GOOGLE_SHEET_CREDENTIALS, SPREADSHEET_ID)

@app.get("/")
async def index():
    return "it works!"

@app.post("/create")
async def create_record(data: DataSchema, db: AsyncSession = Depends(get_db_session)):
    try:
        query = select(DataModel).filter_by(field_1=data.field_1)
        result = await db.execute(query)
        existing = result.scalar_one_or_none()

        if existing:
            raise HTTPException(status_code=400, detail="Record already exists")

        new_data = DataModel(field_1=data.field_1, field_2=data.field_2)
        db.add(new_data)
        await db.commit()
        return {"message": "Record created successfully"}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error creating record: {str(e)}")

@app.get("/read")
async def read_records(db: AsyncSession = Depends(get_db_session)):
    try:
        result = await db.execute(select(DataModel))
        records = result.scalars().all()
        return records
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading records: {str(e)}")

@app.put("/update/{field_1}")
async def update_record(field_1: str, data: DataSchema, db: AsyncSession = Depends(get_db_session)):
    try:
        query = select(DataModel).filter_by(field_1=field_1)
        result = await db.execute(query)
        existing = result.scalar_one_or_none()

        if existing:
            existing.field_2 = data.field_2
            db.add(existing)
            await db.commit()
            return {"message": f"Record {field_1} updated successfully"}
        else:
            raise HTTPException(status_code=404, detail="Record not found")
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error updating record: {str(e)}")

@app.delete("/delete/{field_1}")
async def delete_record(field_1: str, db: AsyncSession = Depends(get_db_session)):
    try:
        query = select(DataModel).filter_by(field_1=field_1)
        result = await db.execute(query)
        existing = result.scalar_one_or_none()

        if existing:
            await db.delete(existing)
            await db.commit()
            return {"message": f"Record {field_1} deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Record not found")
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error deleting record: {str(e)}")
    

@app.get("/sync/google-to-db")
async def sync_google_to_db(db: AsyncSession = Depends(get_db_session)):
    try:
        records = await gsheet_helper.get_all_records()

        if not records:
            return {"message": "No data found in Google Sheets to sync"}

        for record in records:
            query = select(DataModel).filter_by(field_1=record["field_1"])
            result = await db.execute(query)
            existing = result.scalar_one_or_none()

            # Sync Prevention: Check last_synced timestamps to prevent circular sync
            if existing:
                if record.get("last_synced") and existing.last_synced and record["last_synced"] <= existing.last_synced:
                    continue  # Skip if already synced

                # Update existing record
                existing.field_2 = record["field_2"]
                existing.last_synced = func.now()  # Update the last synced timestamp
                db.add(existing)
            else:
                # Insert new record
                new_data = DataModel(
                    field_1=record["field_1"],
                    field_2=record["field_2"],
                    last_synced=func.now()
                )
                db.add(new_data)

        await db.commit()
        return {"message": "Google Sheets data synchronized with the database"}

    except Exception as e:
        logging.error(f"Error syncing Google Sheets to DB: {e}", exc_info=True)
        await db.rollback(c)
        raise HTTPException(status_code=500, detail=f"Error syncing Google Sheets to DB: {str(e)}")

@app.get("/sync/db-to-google")
async def sync_db_to_google(db: AsyncSession = Depends(get_db_session)):
    headers = ["field_1", "field_2"]
    gsheet_helper.create_header(headers)
    try:
        result = await db.execute(select(DataModel))
        db_records = result.scalars().all()

        if not db_records:
            return {"message": "No data found in the database to sync"}

        # Ensure header is present
        headers = ["field_1", "field_2"]
        gsheet_helper.create_header(headers)

        # Clear old data but keep the header intact
        gsheet_helper.clear_sheet()

        # Insert each database record starting from the second row
        for record in db_records:
            # Check if the data in Google Sheets is outdated
            sheet_row = gsheet_helper.find_row_by_value("field_1", record.field_1)
            if sheet_row:
                # Fetch Google Sheets timestamp
                sheet_last_synced = gsheet_helper.get_cell_value(sheet_row, "last_synced")

                if sheet_last_synced and record.last_synced and sheet_last_synced >= record.last_synced:
                    continue  # Skip if already synced

            gsheet_helper.insert_row([record.field_1, record.field_2])

        return {"message": "Database data synchronized with Google Sheets"}

    except Exception as e:
        logging.error(f"Error syncing DB to Google Sheets: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error syncing DB to Google Sheets: {str(e)}")
