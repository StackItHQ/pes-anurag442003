import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession
from unittest.mock import patch, AsyncMock
from ..app.main import app
from app.utils.db_helper import DataModel, get_db_session, init_db

client = TestClient(app)

@pytest.fixture
async def setup_db():
    """Ensure the database is initialized for testing."""
    await init_db()

@pytest.fixture
async def db_session():
    """Get a database session for testing."""
    async for session in get_db_session():
        yield session

# Mock Google Sheet Helper
class MockGoogleSheetHelper:
    def __init__(self, *args, **kwargs):
        pass

    async def get_all_records(self):
        return [{"field_1": "test1", "field_2": "value1"}]

    async def insert_row(self, data, index=1):
        pass

    async def update_cell(self, row, col, value):
        pass

    async def find_row_by_value(self, column_name, value):
        return 2

    async def delete_row(self, index):
        pass

    async def clear_sheet(self):
        pass


### CRUD TEST CASES ###

@pytest.mark.asyncio
async def test_create_record(db_session: AsyncSession):
    # Create a record
    response = client.post("/create", json={"field_1": "test1", "field_2": "value1"})
    assert response.status_code == 200
    assert response.json() == {"message": "Record created successfully"}

    # Ensure record exists in the database
    result = await db_session.execute(
        select(DataModel).filter_by(field_1="test1")
    )
    record = result.scalar_one_or_none()
    assert record is not None
    assert record.field_2 == "value1"

@pytest.mark.asyncio
async def test_read_records(db_session: AsyncSession):
    # Read records from the database
    response = client.get("/read")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0

@pytest.mark.asyncio
async def test_update_record(db_session: AsyncSession):
    # Update an existing record
    response = client.put("/update/test1", json={"field_1": "test1", "field_2": "updated_value"})
    assert response.status_code == 200
    assert response.json() == {"message": "Record test1 updated successfully"}

    # Check if the record was updated
    result = await db_session.execute(
        select(DataModel).filter_by(field_1="test1")
    )
    record = result.scalar_one_or_none()
    assert record.field_2 == "updated_value"

@pytest.mark.asyncio
async def test_delete_record(db_session: AsyncSession):
    # Delete a record
    response = client.delete("/delete/test1")
    assert response.status_code == 200
    assert response.json() == {"message": "Record test1 deleted successfully"}

    # Ensure the record is deleted
    result = await db_session.execute(
        select(DataModel).filter_by(field_1="test1")
    )
    record = result.scalar_one_or_none()
    assert record is None


### SYNC TEST CASES ###

@pytest.mark.asyncio
@patch("app.main.gsheet_helper", new_callable=MockGoogleSheetHelper)
async def test_sync_google_to_db(mock_gsheet_helper, db_session: AsyncSession):
    # Simulate syncing data from Google Sheets to the database
    response = client.get("/sync/google-to-db")
    assert response.status_code == 200
    assert response.json() == {"message": "Google Sheets data synchronized with the database"}

    # Verify that the record was inserted into the database
    result = await db_session.execute(select(DataModel).filter_by(field_1="test1"))
    record = result.scalar_one_or_none()
    assert record is not None
    assert record.field_2 == "value1"

@pytest.mark.asyncio
@patch("app.main.gsheet_helper", new_callable=MockGoogleSheetHelper)
async def test_sync_db_to_google(mock_gsheet_helper, db_session: AsyncSession):
    # Simulate syncing data from the database to Google Sheets
    response = client.get("/sync/db-to-google")
    assert response.status_code == 200
    assert response.json() == {"message": "Database data synchronized with Google Sheets"}

    # Verify that the sync operation was successful (check that the mock was called)
    mock_gsheet_helper.insert_row.assert_called()

@pytest.mark.asyncio
async def test_circular_sync_prevention(db_session: AsyncSession):
    # Insert a record with an old last_synced timestamp
    new_record = DataModel(
        field_1="test1", 
        field_2="old_value", 
        last_synced="2022-01-01 00:00:00"
    )
    db_session.add(new_record)
    await db_session.commit()

    # Simulate a Google Sheet record with a newer timestamp
    mock_gsheet_data = [{"field_1": "test1", "field_2": "new_value", "last_synced": "2023-01-01 00:00:00"}]

    with patch("app.main.gsheet_helper.get_all_records", return_value=mock_gsheet_data):
        response = client.get("/sync/google-to-db")
        assert response.status_code == 200
        assert response.json() == {"message": "Google Sheets data synchronized with the database"}

        # Verify that the record was updated to the new value
        result = await db_session.execute(
            select(DataModel).filter_by(field_1="test1")
        )
        record = result.scalar_one_or_none()
        assert record.field_2 == "new_value"
        assert str(record.last_synced) == "2023-01-01 00:00:00"
