import pytest
from unittest.mock import patch, MagicMock
from app.main import app
import redis
from uuid import uuid4
from pymongo import errors


redis_store = redis.StrictRedis(host='localhost', port=6379, db=1, decode_responses=True)

@pytest.fixture
def client():
    """Fixture to create a Flask test client."""
    with app.test_client() as client:
        yield client

def test_init_upload_success(client):
    total_chunks = 10
    mock_file_id = str(uuid4())
    mock_metadata = {
        "file_id": mock_file_id,
        "total_chunks": total_chunks,
        "chunks_uploaded": 0,
        "status": "Not Started"
    }


    # Mock Redis set
    with patch.object(redis_store, 'hset', return_value=1):
        with patch.object(redis_store, 'hgetall', return_value=mock_metadata):
            response = client.post(f"/content/init-file-upload/{total_chunks}")
            assert response.status_code == 201
            assert response.json == mock_metadata


def test_init_upload_invalid_data(client):
    total_chunks = -1  # Invalid total_chunks

    response = client.post(f"/content/init-file-upload/{total_chunks}")
    assert response.status_code == 400
    assert response.json == {"error": "Invalid data: File metadata not found after saving."}

def test_init_upload_redis_error(client):
    total_chunks = 10

    # Simulate Redis connection error
    with patch.object(redis_store, 'hset', side_effect=redis.exceptions.ConnectionError):
        response = client.post(f"/init-file-upload/{total_chunks}")
        assert response.status_code == 503
        assert response.json == {"error": "Redis connection error"}


def test_get_status_success(client):
    file_id = 'some_file_id'
    mock_metadata = {
        "file_id": file_id,
        "total_chunks": 10,
        "chunks_uploaded": 5,
        "status": "In Progress"
    }

    # Mock Redis call
    with patch.object(redis_store, 'hgetall', return_value=mock_metadata):
        response = client.get(f"/content/files/{file_id}")
        assert response.status_code == 200
        assert response.json == mock_metadata

def test_get_status_not_found(client):
    file_id = 'non_existing_file_id'

    # Mock Redis to return None
    with patch.object(redis_store, 'hgetall', return_value=None):
        response = client.get(f"/files/{file_id}")
        assert response.status_code == 404
        assert response.json == {"error": f"File metadata not found for file_id={file_id}"}

def test_get_status_redis_error(client):
    file_id = '1bbd9993-9aae-4ad0-9513-b1a46a766bdb.csv'
    # Simulate Redis connection error
    with patch.object(redis_store, 'hgetall', side_effect=redis.exceptions.ConnectionError):
        response = client.get(f"/files/{file_id}")
        assert response.status_code == 500
        assert response.json == {"error": "Redis connection error: Redis connection error"}


def test_get_content_success(client):
    mock_content = [
        {"title": "Movie 1", "date_added": "2024-01-01"},
        {"title": "Movie 2", "date_added": "2024-01-02"}
    ]
    
    # Mock the MongoDB collection's `find` method
    with patch('your_flask_app.get_collection') as mock_get_collection:
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection
        mock_collection.find.return_value = mock_content
        
        response = client.get('/content?page=1&per_page=2&sort_by=date_added&sort_order=-1')
        assert response.status_code == 200
        assert response.json == mock_content

def test_get_content_not_found(client):
    # Simulate an empty MongoDB collection
    with patch('your_flask_app.get_collection') as mock_get_collection:
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection
        mock_collection.find.return_value = []

        response = client.get('/content?page=1&per_page=2&sort_by=date_added&sort_order=-1')
        assert response.status_code == 404
        assert response.json == {"message": "No content found."}

def test_get_content_invalid_sort_field(client):
    response = client.get('/content?page=1&per_page=2&sort_by=invalid_field&sort_order=-1')
    assert response.status_code == 400
    assert response.json == {"error": "Invalid sorting field."}

def test_get_content_mongo_error(client):
    # Simulate MongoDB operation failure
    with patch('your_flask_app.get_collection') as mock_get_collection:
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection
        mock_collection.find.side_effect = errors.OperationFailure("MongoDB error")

        response = client.get('/content?page=1&per_page=2&sort_by=date_added&sort_order=-1')
        assert response.status_code == 500
        assert response.json == {"error": "MongoDB operation failed: MongoDB error"}
