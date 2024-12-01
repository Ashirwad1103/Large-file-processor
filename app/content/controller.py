from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required
import os
import traceback
from uuid import uuid4
import redis
import redis.exceptions
from celery_app.tasks import process_chunks
from db.sessions import get_collection
from pymongo import errors
from datetime import datetime
from app.utils.decorator import timer


# Redis setup
redis_store = redis.StrictRedis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    db=int(os.getenv("REDIS_DB_STORE")),
    decode_responses=True
)

# Base directory for file storage
BASE_DIR = os.getenv('UPLOAD_DIR')
os.makedirs(BASE_DIR, exist_ok=True)

# Content Blueprint
content_bp = Blueprint("content", __name__)

@content_bp.route("/create-chunk", methods=["POST"])
@timer
@jwt_required()
def create_chunk():
    """
    Handles file chunk uploads by saving the uploaded chunk to disk, updating
    the metadata in Redis, and triggering a processing task if all chunks
    are uploaded.

    Returns:
        JSON Response:
            - 201: Chunk created successfully.
            - 400: Missing or invalid input fields.
            - 404: File ID not found in Redis.
            - 500: Error during file storage or Redis operations.
    """

    try:
        # Retrieve the input data from the request
        file_id = request.form.get("file_id")
        chunk_id = request.form.get("chunk_id")
        file_chunk = request.files.get('file')

        # Validate input data
        if not file_id or not chunk_id or not file_chunk:
            return jsonify({"error": f"Missing required fields: file_id={file_id}, chunk_id={chunk_id}, file_chunk={type(file_chunk)}"}), 400

        # Check if the file_id exists in Redis
        if not redis_store.exists(file_id):
            return jsonify({"error": f"Invalid file_id: {file_id} not found in Redis"}), 404

        # Create file directory if it doesn't exist
        file_dir = os.path.join(BASE_DIR, file_id)
        try:
            os.makedirs(file_dir, exist_ok=True)
        except OSError as e:
            traceback.print_exc()
            return jsonify({"error": f"Failed to create directory {file_dir}: {str(e)}"}), 500

        # Create the chunk file path
        chunk_path = os.path.join(file_dir, chunk_id)

        # Save the file chunk to disk
        try:
            file_chunk.save(chunk_path)
        except Exception as e:
            traceback.print_exc()
            return jsonify({"error": f"Failed to save file chunk {chunk_id} to disk: {str(e)}"}), 500

        # Update file metadata in Redis
        try:
            with redis_store.pipeline() as pipe:
                while True:
                    try:
                        # Watch the Redis key for changes
                        pipe.watch(file_id)

                        # Get and convert file metadata
                        file_metadata = pipe.hgetall(file_id)
                        file_metadata = {k: int(v) if v.isdigit() else v for k, v in file_metadata.items()}

                        # Increment the chunks_uploaded counter
                        file_metadata["chunks_uploaded"] += 1

                        # Check if all chunks have been uploaded
                        if file_metadata["chunks_uploaded"] == file_metadata["total_chunks"]:
                            file_metadata["status"] = "Processing"

                            # Trigger the processing task (asynchronously)
                            process_chunks.delay(file_id)
                            print(f"Processing task triggered for file_id={file_id}")

                        # Set the updated metadata back to Redis
                        pipe.multi()
                        pipe.hset(file_id, mapping={k: str(v) for k, v in file_metadata.items()})  # Convert back to strings
                        pipe.execute()
                        break
                    except redis.exceptions.WatchError:
                        # If another process modified the key, retry the transaction
                        continue
                    except redis.exceptions.RedisError as e:
                        # Catch Redis-specific errors
                        traceback.print_exc()
                        return jsonify({"error": f"Redis error while updating metadata: {str(e)}"}), 500
        except Exception as e:
            # Handle errors related to Redis pipeline or multi commands
            traceback.print_exc()
            return jsonify({"error": f"Error updating Redis metadata: {str(e)}"}), 500

        # Return success message if all steps succeed
        return jsonify({"message": "Chunk created successfully", "file_id": file_id, "chunk_id": chunk_id}), 201

    except KeyError as e:
        return jsonify({"error": f"Missing required key: {str(e)}"}), 400

    except OSError as e:
        return jsonify({"error": f"File system error: {str(e)}"}), 500

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@content_bp.route("/files/<string:file_id>")
@jwt_required()
def get_status(file_id: str):
    """
    Retrieves the metadata of a file by its ID from Redis, including the upload
    status and total chunks.

    Returns:
        JSON Response:
            - 200: File metadata retrieved successfully.
            - 404: File ID not found in Redis.
            - 500: Redis connection or timeout error.
    """

    try:
        # Fetch file metadata from Redis store
        file_metadata = redis_store.hgetall(file_id)

        if not file_metadata:
            return jsonify({"error": f"File metadata not found for file_id={file_id}"}), 404
        
        return jsonify(file_metadata), 200

    except redis.exceptions.ConnectionError as e:
        return jsonify({"error": f"Redis connection error: {str(e)}"}), 500

    except redis.exceptions.TimeoutError as e:
        return jsonify({"error": f"Redis timeout error: {str(e)}"}), 500

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@content_bp.route("/init-file-upload/<int:total_chunks>", methods=["POST"])
@jwt_required()
def init_upload(total_chunks: int):
    """
    Initializes a file upload by creating metadata in Redis, including the
    total chunks and upload status.

    Returns:
        JSON Response:
            - 201: Upload initialization successful with metadata.
            - 400: Invalid input or missing data.
            - 503: Redis connection error.
            - 500: Internal server error during initialization.
    """

    try:
        if total_chunks < 1: 
            return jsonify({"error": f"Invalid data: total_chunks"}), 400
        # Generate a unique file ID
        
        file_id = str(uuid4())


        # Prepare the file metadata
        file_data = {
            "file_id": file_id,
            "total_chunks": total_chunks,
            "chunks_uploaded": 0,
            "status": "Not Started",
        }

        # Store the metadata in Redis
        redis_store.hset(name=file_id, mapping=file_data)

        # Retrieve and return the stored metadata
        file_metadata = redis_store.hgetall(file_id)
        if not file_metadata:
            raise ValueError("File metadata not found after saving.")

        return jsonify(file_metadata), 201

    except ValueError as e:
        return jsonify({"error": f"Invalid data: {str(e)}"}), 400

    except redis.exceptions.ConnectionError as e:
        return jsonify({"error": "Redis connection error"}), 503

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


# Helper function to parse and validate sorting parameters
def parse_sorting(sort_by: str):
    """
    Validates and returns the sorting field for content retrieval.

    Args:
        sort_by (str): The field to sort by.

    Returns:
        str: Validated sort field.

    Raises:
        ValueError: If the sort field is invalid.
    """

    valid_sort_fields = ["date_added", "release_year", "duration"]
    if sort_by not in valid_sort_fields:
        raise ValueError(f"Invalid sort field. Valid options are {valid_sort_fields}.")
    return sort_by


def format_date_added(docs: list) -> list:
    """
    Formats the 'date_added' field in a list of documents to a readable string format.

    Args:
        docs (list): List of documents to format.

    Returns:
        list: Documents with the formatted 'date_added' field.
    """

    for doc in docs:
        if 'date_added' in doc and isinstance(doc['date_added'], datetime):
            doc['date_added'] = doc['date_added'].strftime("%B %d, %Y").replace(' 0', ' ')
    return docs

# Route to get content with pagination and sorting
@content_bp.route("/content")
@jwt_required()
def get_content():
    """
    Fetches paginated and sorted content from the database. Supports filtering
    by sort fields and order.

    Returns:
        JSON Response:
            - 200: Paginated and sorted content data.
            - 404: No content found.
            - 400: Invalid sorting field.
            - 500: MongoDB operation or internal server error.
    """

    try:
        # Pagination parameters
        page: int = int(request.args.get('page', 1))  # Default to page 1 if not specified
        per_page: int = int(request.args.get('per_page', 10))  # Default to 10 items per page

        # Sorting parameters
        sort_by: str = request.args.get('sort_by', 'date_added')  # Default to 'date_added' if not specified
        sort_order: int = int(request.args.get('sort_order', -1))  # Default to descending order (-1)

        # Validate sorting field
        sort_by: str = parse_sorting(sort_by)

        # Get the collection
        content_collection = get_collection(name="movies_collection")

        # Apply sorting
        sort_field = sort_by
        sort = [(sort_field, sort_order)]

        # Skip and limit for pagination
        skip: int  = (page - 1) * per_page
        limit: int = per_page

        # Fetch the content with sorting and pagination
        content = content_collection.find({}, {"_id": 0}).skip(skip).limit(limit).sort(sort)

        # Convert the results to a list and return them
        content_list = list(content)

        # If the result is empty, return a message indicating no content
        if not content_list:
            return jsonify({"message": "No content found."}), 404
        

        formatted_data: list = format_date_added(docs=content_list)
        
        return jsonify(formatted_data), 200

    except ValueError as e:
        return jsonify({"error": str(e)}), 400  # Invalid sort_by value
    except errors.OperationFailure as e:
        return jsonify({"error": f"MongoDB operation failed: {str(e)}"}), 500
    except Exception as e:
        traceback.print_exc()
        print(f"Unexpected error: {e}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500
