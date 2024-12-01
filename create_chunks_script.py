import asyncio
import csv
import io
import aiohttp
from typing import List
import time
from functools import wraps
import traceback
import os

AUTH_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmcmVzaCI6ZmFsc2UsImlhdCI6MTczMzA2Njk1NywianRpIjoiNDVlZTk4MjQtMDZjYy00NGMxLTlkODktM2VkZjNlM2RlYTcwIiwidHlwZSI6ImFjY2VzcyIsInN1YiI6ImFzaHVAZ21haWwuY29tIiwibmJmIjoxNzMzMDY2OTU3LCJjc3JmIjoiZDQ5NGZkNjktMzc1Mi00YTAyLTk5YTItYThkNGEwODEyMzNiIiwiZXhwIjoxNzMzMDcwNTU3fQ._g67Tapv3RVUm0Vqv3jcFXf4lydFLh4akzNkmHTF2Cc"

def timer(func):
    @wraps(func)  # Preserves the name and docstring of the original function
    async def wrapper(*args, **kwargs):  # Accepts any arguments
        start_time = time.time()
        results = await func(*args, **kwargs)  # Passes arguments to the original function
        time_taken = time.time() - start_time
        print(f"Time Taken | {time_taken=}")
        return results

    return wrapper

def split_csv(file_path, chunk_size):
    chunks: List[str]  = []


    def create_chunk(chunk_rows):
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerows(chunk_rows)  # Write rows to the StringIO buffer
        csv_data = output.getvalue()  # Get the CSV string
        output.close()

        return csv_data

    with open(file_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)  # Skip headers
        chunk_rows = [headers] # Header should be included only in the first chunk
        for row in reader:
            # print(f"{row=}")
            chunk_rows.append(row)
            # checks is no of rows have become more than the limit chunk_size
            # print(f"{len(chunk_rows)=}{chunk_size=}")
            if len(chunk_rows) >= chunk_size:
                try:
                    csv_data = create_chunk(chunk_rows=chunk_rows)
                except Exception as e: 
                    traceback.print_exc()
                    return []
                chunks.append(csv_data) 
                # chunk_rows = [headers]
                chunk_rows = []


        if len(chunk_rows) > 1: # chunk_rows should contain at least one row, other chunk_rows is [headers] 
            csv_data = create_chunk(chunk_rows=chunk_rows) # Get the CSV string
            chunks.append(csv_data)
            # chunks.append(chunk)  # Add remaining rows as the last chunk


    print(f"Generated | {len(chunks)=}")

    return chunks


# Function to send chunk to the API concurrently
async def send_chunk(session, file_id: str, chunk_idx: int, chunk: str):

    url = 'http://127.0.0.1:5000/content/create-chunk'  # Your API URL

    form_data = aiohttp.FormData()
    form_data.add_field('file_id', file_id)
    form_data.add_field('chunk_id', str(chunk_idx))

    chunk_as_file: io.StringIO = io.StringIO(chunk)

    form_data.add_field('file', chunk_as_file, filename=f"chunk_{chunk_idx}.csv", content_type='text/csv')
    
    headers = {
        'Authorization': f'Bearer {AUTH_TOKEN}'  # Include the Bearer token in the headers
    }

    async with session.post(url, data=form_data, headers=headers) as response:
        if response.status == 201:
            print(f"{chunk_idx=} uploaded successfully")
        else:
            print(f"Failed to upload chunk {chunk_idx=}, {response.status=}, {await response.text()}")

# Main function to handle the CSV processing and asynchronous upload
@timer
async def process_and_upload(file_path, file_id: str, chunk_size):
    chunks = split_csv(file_path, chunk_size)

    headers = {
        'Authorization': f'Bearer {AUTH_TOKEN}'  # Include the Bearer token in the headers
    }

    # need to call this after split_csv, because we need the len(chunks)
    if file_id == "":
        async with aiohttp.ClientSession() as session:
            # Step 1: Call init-file-upload API to get the file_id
            init_url = f"http://127.0.0.1:5000/content/init-file-upload/{len(chunks)}"  # Pass the total number of chunks
            async with session.post(init_url, headers=headers) as response:
                # print(f"{response=}")
                status = response.status
                if status == 201:  # Ensure the init-file-upload API succeeds
                    init_data = await response.json()
                    file_id = init_data.get("file_id")
                    print(f"Response | Initialized file upload with file_id: {file_id}")
                else:
                    response = await response.json()
                    print(f"Response | Failed to initialize file upload, {status=}, {response=}")
                    return  # Stop further processing if initialization fails

    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        tasks = []
        for chunk_idx, chunk in enumerate(chunks):
            # Add headers as the first row in each chunk for consistency
            # chunk_with_headers: list  = [headers] + chunk
            task = send_chunk(session=session, file_id=file_id, chunk_idx=chunk_idx, chunk=chunk)
            tasks.append(task)

        # Run all tasks concurrently
        await asyncio.gather(*tasks)

    time_taken = time.time() - start_time
    print(f"Chunk sent time | {time_taken=}")

file_id = ""
chunk_size = 10
file_path = r'E:\Large-file-processor\movies - movies.csv.csv'  # Path to your CSV 

csv_file_name = 'movies - movies.csv.csv'

# Get the root directory (current script's directory)
root_dir = os.getcwd()  # This gets the current working directory where the script runs

# Construct the complete path to the CSV file
csv_file_path = os.path.join(root_dir, csv_file_name)

if __name__ == '__main__':
    asyncio.run(process_and_upload(file_path, file_id=file_id, chunk_size=chunk_size))
