from pymongo import errors
import os
import pandas as pd
import redis
import redis.exceptions
from db.sessions import get_collection




UPLOAD_DIR = os.getenv('UPLOAD_DIR')
MERGE_DIR = os.getenv('MERGE_DIR')

os.makedirs(MERGE_DIR, exist_ok=True)

# redis_store = redis.StrictRedis(host='localhost', port=6379, db=1, decode_responses=True)

redis_store = redis.StrictRedis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    db=int(os.getenv("REDIS_DB_STORE")),
    decode_responses=True
)

def get_metadata(file_id: str) -> dict:
    """
    Fetch metadata for a given file_id from the Redis store.

    Args:
        file_id (str): The ID of the file to fetch metadata for.

    Returns:
        dict: The metadata for the file as a dictionary.

    Raises:
        KeyError: If no metadata is found for the given file_id.
        redis.exceptions.ConnectionError: If there is an issue connecting to the Redis server.
        redis.exceptions.TimeoutError: If the request to Redis times out.
        Exception: For any other unexpected errors.
    """
    try:
        # Fetch metadata from Redis
        file_metadata = redis_store.hgetall(name=file_id)

        if not file_metadata:
            raise KeyError(f"Metadata not found for file_id={file_id}")

        # Convert Redis response (bytes) to a dictionary with string keys and values
        return file_metadata

    except KeyError as e:
        print(f"KeyError: {str(e)}")
    except redis.exceptions.ConnectionError as e:
        print(f"Redis connection error: {str(e)}")
    except redis.exceptions.TimeoutError as e:
        print(f"Redis timeout error: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred while fetching metadata: {str(e)}")


def merge_chunks(file_id: str) -> str:
    """
    Merges file chunks into a single file and cleans up the chunk files and directory.

    Args:
        file_id (str): Unique identifier for the file to be merged.

    Returns:
        str: Path to the merged output file.

    Raises:
        FileNotFoundError: If the chunks directory does not exist.
        ValueError: If no chunk files are found in the directory.
        PermissionError: If there is an issue with file or directory permissions.
        OSError: If an error occurs during file or directory operations.
    """
    try:
        # Construct paths
        chunks_directory: str = os.path.join(UPLOAD_DIR, file_id)
        output_file: str = os.path.join(MERGE_DIR, f"{file_id}.csv")

        # Ensure chunks directory exists
        if not os.path.exists(chunks_directory) or not os.path.isdir(chunks_directory):
            raise FileNotFoundError(f"Chunks directory '{chunks_directory}' not found.")

        # List and sort chunk files
        chunk_files = list(map(
            lambda f: os.path.join(chunks_directory, f), 
            os.listdir(chunks_directory)
        ))

        if not chunk_files:
            raise ValueError(f"No chunk files found in '{chunks_directory}'.")

        chunk_files = sorted(
            chunk_files,
            key=lambda x: int(os.path.basename(x).split('_')[-1])  # Sort by chunk index
        )

        # Merge chunks into a single file
        with open(output_file, "wb") as outfile:
            for chunk_file in chunk_files:
                with open(chunk_file, "rb") as infile:
                    outfile.write(infile.read())

        # Delete chunks after merging
        for chunk_file in chunk_files:
            os.remove(chunk_file)

        # Remove the now-empty chunks directory
        os.rmdir(chunks_directory)

        return output_file

    except FileNotFoundError as e:
        print(f"FileNotFoundError: {e}")
    except ValueError as e:
        print(f"ValueError: {e}")
    except PermissionError as e:
        print(f"PermissionError: {e} - Check file/directory permissions.")
    except OSError as e:
        print(f"OSError: {e} - Issue with file or directory operations.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def prepare_mongo_records(batch: pd.DataFrame):
    """
    Prepares a pandas DataFrame for MongoDB insertion.

    Args:
        batch (pd.DataFrame): DataFrame containing the data to be processed.

    Returns:
        list: A list of dictionaries representing the MongoDB-compatible records.

    Raises:
        TypeError: If the input is not a pandas DataFrame.
        ValueError: If the DataFrame is empty or has no data after processing.
        KeyError: If the DataFrame structure is not as expected.
    """
    try:
        # Validate input
        if not isinstance(batch, pd.DataFrame):
            raise TypeError("Input must be a pandas DataFrame.")
        if batch.empty:
            raise ValueError("The DataFrame is empty and cannot be processed.")

        # Exclude the first row
        # batch = batch.iloc[1:]

        if batch.empty:
            raise ValueError("The DataFrame has no data after excluding the first row.")

        # Replace NaN with None for MongoDB compatibility
        batch = batch.where(pd.notnull(batch), None)

        # Convert 'date_added' column to datetime, so this field can be sorted while fetching records from mongo
        if 'date_added' in batch.columns:
            batch['date_added'] = pd.to_datetime(batch['date_added'], errors='coerce')
        # Convert DataFrame to a list of dictionaries
        records = batch.to_dict(orient="records")

        return records

    except TypeError as e:
        print(f"TypeError: {e}")
    except ValueError as e:
        # traceback.print_exc()
        print(f"ValueError: {e}")
    except KeyError as e:
        print(f"KeyError: {e} - Ensure the DataFrame has the expected structure.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def add_movie_batch_to_mongo(batch: pd.DataFrame):
    """
    Inserts a batch of movie records into a MongoDB collection.

    Args:
        batch (pd.DataFrame): DataFrame containing movie data.

    Raises:
        ValueError: If the data in the batch is invalid.
        errors.BulkWriteError: If an error occurs during batch insertion.
        errors.ServerSelectionTimeoutError: If unable to connect to MongoDB.
        errors.PyMongoError: For any general MongoDB-related errors.
    """
    try:
        # Convert the DataFrame to a list of dictionaries (JSON-like objects)
        records = prepare_mongo_records(batch=batch)

        # Insert the records as a batch
        if records:  # Ensure there is data to insert
            collection = get_collection(name="movies_collection")
            result = collection.insert_many(records)
            print(f"Inserted {len(result.inserted_ids)} records into MongoDB")
        else:
            print("No records to insert")
    
    except ValueError as e:
        print(f"ValueError: Invalid data in the batch - {e}")
    except errors.BulkWriteError as e:
        print(f"BulkWriteError: Issue with batch insertion - {e.details}")
    except errors.ServerSelectionTimeoutError as e:
        print(f"ServerSelectionTimeoutError: Unable to connect to MongoDB - {e}")
    except errors.PyMongoError as e:
        print(f"PyMongoError: An error occurred while interacting with MongoDB - {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def update_redis_metadata(file_id: str, status: str):
    """
    Updates the status of a file's metadata in Redis.

    Args:
        file_id (str): Unique identifier for the file.
        status (str): New status value to be updated.

    Raises:
        KeyError: If no metadata exists for the provided file ID.
        ValueError: If inputs are invalid.
        redis.exceptions.ConnectionError: If there is an issue connecting to Redis.
    """
    try:
        # Ensure valid inputs
        if not file_id or not isinstance(file_id, str):
            raise ValueError("file_id must be a non-empty string.")
        if not isinstance(status, str):
            raise ValueError("status must be a string.")

        # Retrieve metadata for the given file ID
        metadata = redis_store.hgetall(name=file_id)
        if not metadata:  # Check if metadata exists
            raise KeyError(f"No metadata found for file ID: {file_id}")

        # Update the status field
        metadata["status"] = status

        # Write the updated metadata back to Redis
        redis_store.hset(name=file_id, mapping=metadata)
        print(f"Updated status for file ID {file_id} to '{status}'.")
    
    except KeyError as e:
        print(f"KeyError: {e}")
    except ValueError as e:
        print(f"ValueError: {e}")
    except redis.exceptions.ConnectionError as e:
        print(f"Redis ConnectionError: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

