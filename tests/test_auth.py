import pytest
from unittest.mock import patch
from werkzeug.security import generate_password_hash
from app.main import app


@pytest.fixture
def client():
    """Fixture to create a Flask test client."""
    with app.test_client() as client:
        yield client


@patch('app.auth.controller.get_collection')
def test_signup_successful(mock_get_collection, client):
    """Test successful signup."""
    mock_collection = mock_get_collection.return_value
    mock_collection.find_one.return_value = None
    mock_collection.insert_one.return_value.acknowledged = True

    response = client.post('/auth/signup', json={
        "email": "test@example.com",
        "password": "password123"
    })
    assert response.status_code == 201
    assert "User created successfully!" in response.json.get('message')


@patch('app.auth.controller.get_collection')
def test_signup_existing_user(mock_get_collection, client):
    """Test signup with an existing email."""
    mock_collection = mock_get_collection.return_value
    mock_collection.find_one.return_value = {"email": "test@example.com"}

    response = client.post('/auth/signup', json={
        "email": "test@example.com",
        "password": "password123"
    })
    assert response.status_code == 409
    assert "User already exists." in response.json.get('error')


@patch('app.auth.controller.get_collection')
def test_signup_missing_fields(mock_get_collection, client):
    """Test signup with missing email or password."""
    response = client.post('/auth/signup', json={
        "email": "test@example.com"
    })
    assert response.status_code == 400
    assert "Email and password are required." in response.json.get('error')


@patch('app.auth.controller.get_collection')
def test_login_successful(mock_get_collection, client):
    """Test successful login."""
    mock_collection = mock_get_collection.return_value
    mock_collection.find_one.return_value = {
        "email": "test@example.com",
        "password": generate_password_hash("password123")
    }

    response = client.post('/auth/login', json={
        "email": "test@example.com",
        "password": "password123"
    })
    assert response.status_code == 200
    print(f"{response.json=}")
    assert "Login successful!" in response.json.get('message')
    assert "token" in response.json


@patch('app.auth.controller.get_collection')
def test_login_invalid_password(mock_get_collection, client):
    """Test login with an invalid password."""
    mock_collection = mock_get_collection.return_value
    mock_collection.find_one.return_value = {
        "email": "test@example.com",
        "password": generate_password_hash("password123")
    }

    response = client.post('/auth/login', json={
        "email": "test@example.com",
        "password": "wrongpassword"
    })
    assert response.status_code == 401
    assert "Invalid email or password." in response.json.get('error')


@patch('app.auth.controller.get_collection')
def test_login_user_not_found(mock_get_collection, client):
    """Test login for a non-existent user."""
    mock_collection = mock_get_collection.return_value
    mock_collection.find_one.return_value = None

    response = client.post('/auth/login', json={
        "email": "nonexistent@example.com",
        "password": "password123"
    })
    assert response.status_code == 404
    assert "User not found." in response.json.get('error')


@patch('app.auth.controller.get_collection')
def test_login_missing_fields(mock_get_collection, client):
    """Test login with missing email or password."""
    response = client.post('/auth/login', json={
        "email": "test@example.com"
    })
    assert response.status_code == 400
    assert "Email and password are required." in response.json.get('error')
