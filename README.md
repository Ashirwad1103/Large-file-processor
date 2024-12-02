# Large-file-processor

Overview:
This project is designed to handle the efficient uploading, chunk-based processing, and merging of large files. It incorporates concurrent requests for uploading file chunks, task queuing for asynchronous processing, and a robust backend powered by Flask, Celery, and Redis.

Features
    1. Chunk-Based Uploads: Large files are split into manageable chunks for upload.
    2. Concurrent Processing: Concurrent API calls handle chunk uploads simultaneously.
    3. Asynchronous Processing: Task queue ensures non-blocking processing of chunks using Celery.
    4. Storage Management: Temporary chunk storage followed by merging into the final file.
    5. Fault Tolerance: Redis stores metadata to monitor upload and processing progress.
    6. High Scalability: Designed to scale across multiple workers.

Requirements
    Software - 
        1. Python 3.8+
        2. Redis
        3. MongoDb

    Python Libraries
        1. Flask
        2. Celery
        3. Redis
        4. Waitress
        5. Requests

Assumptions

    1. Disk Space: Sufficient disk space is available to store temporary file chunks and the merged file.
    2. Memory: Metadata (stored in Redis) fits within the system’s memory constraints.
    3. Network: Stable network connection for concurrent chunk uploads.
    4. Concurrency: System supports high concurrency levels without overwhelming resources.


SETUP - 

    1. Create a .env file, using the sample given below.

        # Redis configuration
        REDIS_HOST=localhost
        REDIS_PORT=6379
        REDIS_DB_STORE=1  # Redis database for general storage
        REDIS_DB_BROKER=0  # Redis database for Celery broker

        # MongoDB configuration
        MONGO_URI=mongodb://localhost:27017/
        MONGO_DB=movies_database
        JWT_SECRET_KEY=5zwMNKZym9hl1vS4LwJvQ5NCbVFxKT1shFBXjEXXoHM


        UPLOAD_DIR=file_chunks
        MERGE_DIR=merged_files
        CHUNK_SIZE=10


    2. Install Python Dependencies
        pip install -r requirements.txt

    3. Start Redis Server
        redis-server

    4. Start Celery Workers
        celery -A celery_app.tasks worker --pool threads --concurrency 4 --loglevel=info

    5. Run the Flask Application
        python app/main.py

    6. Run the Application with Waitress (for production)
        waitress-serve --host=127.0.0.1 --port=5000 app.main:app



Tests - 

Pytest Command - pytest -s 

1.  tests/test_auth.py - Auth tests 
    Running tests will print Auth token on console.

2. Use the create_chunks_script.py to test sending chunks
    Use the Auth_Token received from the above tests
    Replace AUTH_TOKEN present on line "11".



API Endpoints

    1. Initialize File Upload
        Endpoint: /content/init-file-upload/<int:total_chunks>
        Method: GET

        Description: Registers a new file upload, storing metadata in Redis.
        Response: file_id and metadata for tracking the upload.

    2. Upload Chunk
        Endpoint: /content/create-chunk
        Method: POST
        Payload: {}
        file_id: Unique file identifier.
        chunk_id: Index of the current chunk.
        file: Binary chunk data.

        Description: Uploads a specific chunk of the file.

    3. Process Uploaded Chunks
        Task: Triggered automatically after chunks are uploaded.
        Action: Merges chunks and performs processing.

    4. Login
        Endpoint: /auth/login
        Method: POST

        Payload: {
        "email": "<user_email>",
        "password": "<user_password>"
        }
        Description - 
        Validates user credentials and generates a JWT token for authenticated access.

        Response
            Success: {
            "message": "Login successful!",
            "token": "<jwt_token>"
            }
            Failure:
                400: Missing email or password.
                404: User not found.
                401: Invalid email/password combination.
                Implementation Notes
                Store the JWT secret in environment variables for security.
                Use a library like flask-jwt-extended to manage JWT tokens.
                Set token expiration for added security (e.g., 24 hours).

    5. Signup
        Endpoint: /auth/signup
        Method: POST

        Payload = {
        "email": "<user_email>",
        "password": "<user_password>"
        }

        Description -
        Registers a new user by storing their email and hashed password in the database.

        Response -
            Success: {
            "message": "User created successfully!"
            }

            Failure:
            400: Missing email or password.
            409: User already exists.
            500: Database error or unexpected failure.

            Implementation Notes
            Validate email format and enforce strong password requirements.
            Use secure hashing algorithms like pbkdf2:sha256.

    6. Get Status of File
        Endpoint: /content/files/<string:filed_id>
        Method: GET 

        Requires: Jwt auth token in headers

        Description - 
        Gets the status of processing of file.

        Response -
            Success: {
                "status": "completed",
                "total_chunks": "5",
                "file_id": "some_file_id", 
                "uploaded_chunks": 5
            }
            Failure:
                404: File metadata not found for the provided file_id.
                    Example:
                    {
                        "error": "File metadata not found for file_id=<file_id>"
                    }

                500: Redis connection error, timeout, or unexpected internal server error.
                    Example:
                    {
                        "error": "Redis connection error: <error_details>"
                    }

    7. Get Paginated and Sorted Content
        Endpoint: /content
        Method: GET

        Description:
        Fetches paginated and sorted content from the database. Supports filtering by sorting fields and order.

        Headers:
        Authorization: Bearer <JWT_TOKEN>

        Query Parameters:

        page (optional): Page number for pagination. Default is 1.
        per_page (optional): Number of items per page. Default is 10.
        sort_by (optional): Field to sort by. Default is date_added.
        sort_order (optional): Sorting order, where -1 is descending (default), and 1 is ascending.
        Response:

        Success (200):
        Returns a paginated list of content.
        Example:
        [
            {
                "show_id": "s246",
                "type": "TV Show",
                "title": "Korean Cold Noodle Rhapsody",
                "director": null,
                "cast": "Paik Jong-won",
                "country": null,
                "date_added": "September 02, 2001"
                "release_year": 2021,
                "rating": "TV-PG",
                "duration": "1 Season",
                "listed_in": "Docuseries, International TV Shows",
                "description": "Refreshing and flavorful, naengmyeon is Korea's coolest summertime staple. A journey through its history begins, from how it's cooked to how it's loved."
            },
            {
                "show_id": "s247",
                "type": "Movie",
                "title": "Man in Love",
                "director": "Yin Chen-hao",
                "cast": "Roy Chiu, Ann Hsu, Tsai Chen-nan, Chung Hsin-ling, Lan Wei-hua, Peace Yang, Huang Lu Tz-yin",
                "country": null,
                "date_added": "September 01, 2001"
                "release_year": 2021,
                "rating": "TV-MA",
                "duration": "115 min",
                "listed_in": "Dramas, International Movies, Romantic Movies",
                "description": "When he meets a debt-ridden woman who's caring for her ailing father, a debt collector with a heart of gold sets out to win her love."
            }
        ]

        Failure:

        404: No content found for the given query parameters.
        Example:
        {
            "message": "No content found."
        }

        400: Invalid sorting field or input value
        Example:
        {
            "error": "Invalid sorting field."
        }

        500: MongoDB operation failure or unexpected internal server error.
        Example:
        {
            "error": "MongoDB operation failed: <error_details>"
        }



Design Considerations
    1. Chunk Size: Ensure the chunk size balances upload speed and server processing time.
    2. Concurrency Limits: Restrict concurrent uploads to prevent resource exhaustion.
    3. Error Handling: Retries are implemented for failed chunk uploads.
    4. Monitoring: Use Redis to track the status of each file upload.


Future Enhancements
    1. Cloud Storage: Store chunks and final files in S3 or equivalent.
    2. Streaming: Support real-time chunk streaming.
    3. Enhanced Fault Tolerance: Handle Redis or worker failures more gracefully.

