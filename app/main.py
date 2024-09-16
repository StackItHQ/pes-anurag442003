from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional
from app.database import DatabaseManager
from app.sheets_sync import read_sheet, write_sheet, get_sheet_structure
import asyncio
from datetime import datetime, timedelta

app = FastAPI()


db = DatabaseManager(dbname='anurag_db', user='postgres', password='abb442003', host='localhost', port='5432')

SHEET_ID = '12NMmCimnsY7hzB7fBWgRHqCxbINAaAvG8Tgnsf262Uk' 
RANGE_NAME = 'Sheet1!A:Z'  
SYNC_INTERVAL = 10  
class DataItem(BaseModel):
    id: Optional[int]
data_item_fields = {}

def create_table_from_sheet_structure():
    global data_item_fields
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


    for col in column_names:
        if col.lower() != 'id':
            data_item_fields[col] = (str, ...)
    DataItem.update_forward_refs()

@app.get("/")
async def index():
    return "WElcome!!"

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
    new_id = db.create('data_table', item.dict(exclude={'id'}))
    background_tasks.add_task(sync_data)
    return {"id": new_id}

@app.get("/data")
async def read_data():
    return db.read('data_table')

@app.put("/data/{item_id}")
async def update_data(item_id: int, item: DataItem, background_tasks: BackgroundTasks):
    update_data = item.dict(exclude={'id'}, exclude_unset=True)
    updated_rows = db.update('data_table', item_id, update_data)
    if updated_rows == 0:
        raise HTTPException(status_code=404, detail="Item not found")
    background_tasks.add_task(sync_data)
    return {"message": "Item updated successfully"}

@app.delete("/data/{item_id}")
async def delete_data(item_id: int, background_tasks: BackgroundTasks):
    deleted_rows = db.delete('data_table', item_id)
    if deleted_rows == 0:
        raise HTTPException(status_code=404, detail="Item not found")
    background_tasks.add_task(sync_data)
    return {"message": "Item deleted successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)