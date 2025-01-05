import requests
import boto3
import json
from datetime import datetime

#API datails
API_KEY = "2630d3418ac048d68674c78a39efaa7a"
CITY = "Colombo"
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

#s3
BUCKET_NAME = "dataingestionpipeline"
S3_FOLDER = "weather-data/"


# Function to fetch weather data
def fetch_weather_data(city, api_key):
    params = {"q": city, "appid": api_key, "units": "metric"}
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None


# Function to upload data to S3
def upload_to_s3(data, bucket_name, folder_name):
    s3 = boto3.client('s3')
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"{folder_name}weather_{timestamp}.json"

    # Save data to S3
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    print(f"File uploaded to S3: {file_name}")


# Main Function
if __name__ == "__main__":
    # Step 1: Fetch Weather Data
    weather_data = fetch_weather_data(CITY, API_KEY)

    if weather_data:
        print(f"Weather Data Fetched for {CITY}")

        # Step 2: Upload Data to S3
        upload_to_s3(weather_data, BUCKET_NAME, S3_FOLDER)
    else:
        print("Failed to fetch weather data.")