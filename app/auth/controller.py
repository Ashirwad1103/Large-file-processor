from flask import Flask, request, jsonify, Blueprint
from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import create_access_token
from pymongo import errors
import traceback
from db.sessions import get_collection


auth_bp = Blueprint('auth', __name__)

# MongoDB connection setup
# client = MongoClient("mongodb://localhost:27017/")
# db = client["your_database_name"]
# collection = db["users"]


@auth_bp.route("/signup", methods=["POST"])
def signup():
    """
    Handles user signup by creating a new user in MongoDB after validating
    that the email is not already registered. The password is hashed before storage.

    Steps:
    1. Parse and validate the email and password from the request body.
    2. Check if the email already exists in the database.
    3. Hash the password using a secure hashing method.
    4. Insert the new user record into the MongoDB database.

    Returns:
        JSON Response:
            - 201: On successful user creation.
            - 400: If email or password is missing.
            - 409: If the email is already registered.
            - 500: If a database or unexpected error occurs.

    Raises:
        None: All exceptions are handled and logged within the function.
    """

    try:
        # Parse email and password from the request body
        data: dict = request.get_json()
        email: str = data.get("email")
        password: str = data.get("password")

        # Validate email and password
        if not email or not password:
            return jsonify({"error": "Email and password are required."}), 400

        # Check if the email already exists in the database
        collection = get_collection(name="users")
        existing_user = collection.find_one({"email": email})
        if existing_user:
            return jsonify({"error": "User already exists."}), 409

        # Hash the password
        hashed_password = generate_password_hash(password, method="pbkdf2:sha256")

        # Create the user in MongoDB
        new_user = {"email": email, "password": hashed_password}
        result = collection.insert_one(new_user)
        if not result.acknowledged:
            print("Failed to insert the user into MongoDB.")
            return jsonify({"error": "Failed to create user. Please try again."}), 500

        return jsonify({"message": "User created successfully!"}), 201

    except errors.ConnectionFailure as e:
        print(f"MongoDB connection error: {str(e)}")
        return jsonify({"error": "Database connection error. Please try again later."}), 500

    except errors.OperationFailure as e:
        print(f"MongoDB operation failure: {str(e)}")
        return jsonify({"error": "Database operation failed. Please contact support."}), 500

    except KeyError as e:
        print(f"KeyError: {str(e)} - Missing required field in the request body.")
        return jsonify({"error": f"Missing required field: {str(e)}"}), 400

    except Exception as e:
        traceback.print_exc()
        print(f"Unexpected error: {str(e)}")
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@auth_bp.route("/login", methods=["POST"])
def login():
    """
    Logs in the user by validating their email and password against the stored
    credentials in MongoDB. Returns a JWT token on successful authentication.

    Steps:
    1. Parse and validate the email and password from the request body.
    2. Retrieve the user record from the database based on the email.
    3. Verify the provided password matches the hashed password in the database.
    4. Generate and return a JWT token for the authenticated user.

    Returns:
        JSON Response:
            - 200: On successful login with the generated JWT token.
            - 400: If email or password is missing.
            - 401: If the email or password is invalid.
            - 404: If the email is not found in the database.
            - 500: If a database or unexpected error occurs.

    Raises:
        None: All exceptions are handled and logged within the function.
    """
    try:
        # Parse email and password from the request body
        data: dict = request.get_json()
        email: str = data.get("email")
        password: str = data.get("password")

        # Validate email and password
        if not email or not password:
            return jsonify({"error": "Email and password are required."}), 400

        collection = get_collection(name="users")
        # Check if the user exists in the database
        user = collection.find_one({"email": email})
        if not user:
            return jsonify({"error": "User not found."}), 404

        # Check if the password matches
        if not check_password_hash(user["password"], password):
            return jsonify({"error": "Invalid email or password."}), 401

        # Generate JWT token
        token = create_access_token(identity=email)

        return jsonify({"message": "Login successful!", "token": token}), 200

    except errors.ConnectionFailure as e:
        print(f"MongoDB connection error: {str(e)}")
        return jsonify({"error": "Database connection error. Please try again later."}), 500

    except errors.OperationFailure as e:
        print(f"MongoDB operation failure: {str(e)}")
        return jsonify({"error": "Database operation failed. Please contact support."}), 500

    except KeyError as e:
        print(f"KeyError: {str(e)} - Missing required field in the request body.")
        return jsonify({"error": f"Missing required field: {str(e)}"}), 400

    except Exception as e:
        traceback.print_exc()
        print(f"Unexpected error: {str(e)}")
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

