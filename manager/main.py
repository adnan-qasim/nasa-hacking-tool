from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
import requests, time
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler

app = FastAPI()

# MongoDB connection
client = MongoClient(
    "mongodb+srv://sniplyuser:NXy7R7wRskSrk3F2@cataxprod.iwac6oj.mongodb.net/?retryWrites=true&w=majority"
)
db = client["progress_tracker"]
stuck_collection = db["stuck"]


# Define the request model
class ServerRequest(BaseModel):
    current_server_url: str
    server_name: str
    start: int
    end: int
    backup_server_url: Optional[str] = None


# Define the response model
class ApiResponse(BaseModel):
    server_name: str
    response: dict


# This will store the scheduler globally
scheduler = BackgroundScheduler()


@app.post("/schedule-data/", response_model=List[ApiResponse])
def fetch_data(server_requests: List[ServerRequest]):
    responses = []

    for request in server_requests:
        time.sleep(30)
        try:
            # Make synchronous API call to the external API for each server in the list
            api_response = requests.get(
                request.current_server_url,
                params={
                    "server_name": request.server_name,
                    "start_index": request.start,
                    "end_index": request.end,
                    "backup_server_url": request.backup_server_url,
                    "current_server_url": request.current_server_url,
                },
            )
            # Check if the request was successful
            if api_response.status_code == 200:
                responses.append(
                    ApiResponse(
                        server_name=request.server_name, response=api_response.json()
                    )
                )
            else:
                responses.append(
                    ApiResponse(
                        server_name=request.server_name,
                        response={"error": "Failed to fetch data"},
                    )
                )
        except Exception as e:
            responses.append(
                ApiResponse(server_name=request.server_name, response={"error": str(e)})
            )

    return responses


# Define a background job to run every 10 minutes
def background_task():
    all_stuck_servers = list(stuck_collection.find({"status": "stuck"}))

    for stuck_server in all_stuck_servers:
        # Make a request to the external API to check if the server is back online
        try:
            response = requests.get(
                stuck_server["backup_server_url"],
                params={
                    "server_name": stuck_server["server_name"],
                    "start_index": stuck_server["start"],
                    "end_index": stuck_server["end"],
                    "is_backup": True,
                },
            )
            if response.status_code == 200:
                # If the server is back online, update the status in the database
                stuck_collection.update_one(
                    {"_id": stuck_server["_id"]}, {"$set": {"status": "running"}}
                )
        except Exception as e:
            print(f"Error checking server status: {str(e)}")


# Function to ping the server
def ping_server():
    try:
        list_to_ping = [
            "https://adnan-worker1.onrender.com/",
            "https://adnan-worker2.onrender.com/",
            "https://adnan-worker3.onrender.com/",
            "https://adnan-worker4.onrender.com/",
            "https://adnan-worker5.onrender.com/",
            "https://adnan-worker6.onrender.com/",
            "https://adnan-manager.onrender.com/",
        ]
        for server_url in list_to_ping:
            response = requests.get(server_url)
    except Exception as e:
        print(f"Error pinging server: {e}")


scheduler.add_job(ping_server, "interval", minutes=3)
scheduler.add_job(background_task, "interval", minutes=10)
scheduler.start()


@app.get("/")
def health_check():
    return {"message": "Pinged manager node"}


@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()
