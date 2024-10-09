from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import requests
from apscheduler.schedulers.background import BackgroundScheduler

app = FastAPI()

# Define the request model
class ServerRequest(BaseModel):
    server_name: str
    start: str
    end: str

# Define the response model
class ApiResponse(BaseModel):
    server_name: str
    response: dict

# Dummy URL for external API (replace with actual URL)
EXTERNAL_API_URL = "http://external.api/endpoint"

# This will store the scheduler globally
scheduler = BackgroundScheduler()

@app.post("/fetch-data/", response_model=List[ApiResponse])
def fetch_data(server_requests: List[ServerRequest]):
    responses = []
    
    for request in server_requests:
        try:
            # Make synchronous API call to the external API for each server in the list
            api_response = requests.get(
                EXTERNAL_API_URL,
                params={
                    "server_name": request.server_name,
                    "start": request.start,
                    "end": request.end
                }
            )
            # Check if the request was successful
            if api_response.status_code == 200:
                responses.append(ApiResponse(server_name=request.server_name, response=api_response.json()))
            else:
                responses.append(ApiResponse(server_name=request.server_name, response={"error": "Failed to fetch data"}))
        except Exception as e:
            responses.append(ApiResponse(server_name=request.server_name, response={"error": str(e)}))
    
    return responses

# Define a background job to run every 10 minutes
def background_task():
    print("Background task is running...")  # Replace with actual code to be executed
    # For example, making an API call or processing data

# Start the scheduler and add the background job
scheduler.add_job(background_task, 'interval', minutes=10)
scheduler.start()

# Shutdown the scheduler when the application stops
@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()
