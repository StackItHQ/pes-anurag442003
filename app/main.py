from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import List, Optional
from app.database import DatabaseManager
from app.sheets_sync import read_sheet, write_sheet, get_sheet_structure
import asyncio
from datetime import datetime, timedelta
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import logging
import os

load_dotenv()
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)
app.mount("/static", StaticFiles(directory="app/static"), name="static")



db = DatabaseManager(
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT')
)

SHEET_ID = os.getenv('SHEET_ID')
RANGE_NAME = 'Sheet1!A:Z'  
SYNC_INTERVAL = 10  

class DataItem(BaseModel):
    id: Optional[int] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    department: Optional[str] = None
    hire_date: Optional[str] = None

def create_table_from_sheet_structure():
    sheet_structure = get_sheet_structure(SHEET_ID)
    if not sheet_structure:
        raise Exception("Failed to get sheet structure")

    first_sheet_name = list(sheet_structure.keys())[0]
    column_count = sheet_structure[first_sheet_name]

    default_headers = ['id', 'first_name', 'last_name', 'email', 'department', 'hire_date']

    first_row = read_sheet(SHEET_ID, f'{first_sheet_name}!A1:{chr(65+len(default_headers)-1)}1')
    if not first_row or not first_row[0]:
        write_sheet(SHEET_ID, f'{first_sheet_name}!A1:{chr(65+len(default_headers)-1)}1', [default_headers])
        column_names = default_headers
    else:
        column_names = first_row[0]

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS data_table (
        id SERIAL PRIMARY KEY,
        {', '.join(f'"{col}" TEXT' for col in column_names if col.lower() != 'id')},
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    db.cur.execute(create_table_query)
    db.conn.commit()

    print(f"Created table with columns: {', '.join(column_names)}")

@app.on_event("startup")
async def startup_event():
    create_table_from_sheet_structure()
    asyncio.create_task(periodic_sync())

async def periodic_sync():
    while True:
        await sync_data()
        await asyncio.sleep(SYNC_INTERVAL)

async def sync_data():

    last_sync = db.get_last_sync_time()


    sheet_data = read_sheet(SHEET_ID, RANGE_NAME)
    if sheet_data:
        headers = sheet_data[0]
        for row in sheet_data[1:]:
            data = dict(zip(headers, row))
    
            existing_row = db.read('data_table', data.get('id'))
            if existing_row:
              
                if any(existing_row[key] != value for key, value in data.items() if key != 'id'):
                    db.update('data_table', data['id'], data)
            else:
                
                db.create('data_table', data)


    db_data = db.read('data_table')
    sheet_data = [headers]  
    for row in db_data:
        sheet_data.append([row[header] for header in headers])
    write_sheet(SHEET_ID, RANGE_NAME, sheet_data)


    db.update_last_sync_time()

@app.post("/data")
async def create_data(item: DataItem, background_tasks: BackgroundTasks):
    try:
        # Get the last row number
        last_row = db.read('data_table')
        new_id = len(last_row) + 1 if last_row else 1
        
        # Create new item with the calculated ID
        new_data = item.dict(exclude_unset=True)
        new_data['id'] = new_id
        db.create('data_table', new_data)
        
        background_tasks.add_task(sync_data)
        return {"id": new_id}
    except Exception as e:
        logging.error(f"Error creating data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data")
async def read_data():
    return db.read('data_table')

@app.put("/data/{item_id}")
async def update_data(item_id: int, item: DataItem, background_tasks: BackgroundTasks):
    try:
        existing_data = db.read('data_table', item_id)
        if not existing_data:
            raise HTTPException(status_code=404, detail="Item not found")
        
        update_data = item.dict(exclude_unset=True)
        updated_id = db.update('data_table', item_id, update_data)
        background_tasks.add_task(sync_data)
        return {"message": f"Item {updated_id} updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error updating data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/data/{item_id}")
async def delete_data(item_id: int, background_tasks: BackgroundTasks):
    try:
        deleted_id = db.delete('data_table', item_id)
        if not deleted_id:
            raise HTTPException(status_code=404, detail="Item not found")
        background_tasks.add_task(sync_data)
        return {"message": f"Item {deleted_id} deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error deleting data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)