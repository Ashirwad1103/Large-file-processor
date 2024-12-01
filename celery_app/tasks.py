from celery import Celery
import redis
import pandas as pd
from celery_app.utils import merge_chunks, add_movie_batch_to_mongo, update_redis_metadata, get_metadata
from dotenv import load_dotenv
import os

load_dotenv()

CHUNK_SIZE = int(os.getenv('CHUNK_SIZE'))

# COMMAND | 
# celery -A celery_app.tasks worker --pool threads --concurrency 4 --loglevel=info --without-gossip --without-mingle




broker_url = f"redis://{os.getenv('REDIS_HOST')}:{os.getenv('REDIS_PORT')}/{os.getenv('REDIS_DB_BROKER')}"
backend_url = broker_url

# Initialize Celery app with Redis as the broker
app = Celery(
    'tasks',
    broker=broker_url,
    backend=backend_url
)


@app.task
def process_chunks(file_id: str):
    """
    Processes file chunks for a given file ID by merging them, processing the resulting CSV, 
    and updating metadata in Redis.

    Steps:
    1. Retrieve file metadata from Redis.
    2. Merge file chunks into a single CSV file.
    3. Process the CSV file in chunks, inserting data into MongoDB.
    4. Update Redis metadata with the status of processing.

    Args:
        file_id (str): Unique identifier for the file to process.

    Returns:
        int: HTTP-like status code indicating the result:
            - 200: Success.
            - 500: Internal Server Error in case of unexpected exceptions.

    Raises:
        None: All exceptions are handled and logged within the function.
    """
    try:
        print(f"Processing | {file_id=}")

        # Retrieve metadata from Redis
        file_metadata: dict = get_metadata(file_id=file_id)
        if not file_metadata:
            print(f"ERROR | metadata not found for {file_id=}")
            return  # Exit early if no metadata

        # Merge chunks into a single CSV file
        try:
            csv_file_path: str = merge_chunks(file_id=file_id)
        except Exception as e:
            print(f"ERROR | Failed to merge chunks for {file_id=}: {e}")
            return  # Exit if merging fails

        # Process CSV in chunks
        try:
            for batch in pd.read_csv(csv_file_path, chunksize=CHUNK_SIZE, header=0):
                add_movie_batch_to_mongo(batch=batch)
                # Update rows_processed count in Redis (to be implemented)
        except FileNotFoundError as e:
            print(f"ERROR | CSV file not found: {e}")
            return
        except pd.errors.ParserError as e:
            print(f"ERROR | Error while reading CSV file: {e}")
            return
        except Exception as e:
            print(f"ERROR | Unexpected error while processing CSV: {e}")
            return

        # Update the status in Redis after processing
        try:
            update_redis_metadata(file_id=file_id, status="Completed")
        except Exception as e:
            print(f"ERROR | Failed to update Redis metadata for {file_id=}: {e}")
            return  # Exit if updating status fails

        print(f"Completed processing for {file_id=}")
        return 200  # Success

    except Exception as e:
        print(f"ERROR | Unexpected error occurred during process_chunks: {e}")
        return 500  # Internal Server Error
