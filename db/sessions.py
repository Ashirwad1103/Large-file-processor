from pymongo import MongoClient, errors
import os
from dotenv import load_dotenv

load_dotenv()


mongo_uri = os.getenv("MONGO_URI")
mongo_db_name = os.getenv("MONGO_DB")
# Establish the MongoDB connection
try:
    client = MongoClient(mongo_uri)
    db = client[mongo_db_name]
except errors.ConnectionFailure as e:
    print(f"Error connecting to MongoDB: {e}")
    raise  # Reraise the exception or handle it as needed
except errors.ConfigurationError as e:
    print(f"MongoDB configuration error: {e}")
    raise
except Exception as e:
    print(f"Unexpected error: {e}")
    raise



# Function to get collection with exception handling
def get_collection(name: str):
    try:
        collection = db[name]
        return collection
    except errors.InvalidName as e:
        print(f"Invalid collection name: {e}")
        raise  # Reraise the exception or handle it as needed
    except errors.OperationFailure as e:
        print(f"MongoDB operation failure: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error in get_collection: {e}")
        raise
