import requests
import json
import time
import os
from google.cloud import storage
from datetime import datetime

def fetch_hospitals(city, api_key):
    base_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    all_results = []

    params = {
        'query': f'hospitals in {city}',
        'key': api_key,
        'type': 'hospital'
    }

    try:
        response = requests.get(base_url, params=params)
        data = response.json()
    except Exception as e:
        print(f"Error fetching data from the API for {city}: {e}")
        return None

    if data.get('status') not in ["OK", "ZERO_RESULTS"]:
        print(f"API Error for {city}: {data.get('status')}. Please check your API key and city name.")
        return None

    if data.get("results"):
        all_results.extend(data["results"])
    else:
        print(f"No hospitals found in {city}.")
        return []

    while 'next_page_token' in data:
        time.sleep(2)
        params = {
            'pagetoken': data['next_page_token'],
            'key': api_key
        }
        try:
            response = requests.get(base_url, params=params)
            data = response.json()
        except Exception as e:
            print(f"Error fetching additional pages for {city}: {e}")
            break

        if data.get('status') == "OK":
            all_results.extend(data.get('results', []))
        else:
            break

    return all_results

def upload_to_gcs(bucket_name, data, destination_blob_name):
    """Uploads a JSON string to Google Cloud Storage inside the specified folder."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob_path = os.path.join(destination_blob_name) # Ensure proper path joining
        print(f"Destination Blob Name: {blob_path}") # Added logging for debugging
        blob = bucket.blob(blob_path)
        blob.upload_from_string(json.dumps({"results": data}, indent=4, ensure_ascii=False), 'application/json')
        print(f"JSON data uploaded to {bucket_name}/{blob_path}.")
        return f"gs://{bucket_name}/{blob_path}"
    except Exception as e:
        print(f"Error uploading to GCS: {e}")
        return None

def main(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    api_key = request_json.get('api_key')
    city = request_json.get('city')
    gcs_bucket_name = request_json.get('bucket_name')
    folder_name = request_json.get('folder', 'lists')  # Get folder name, default to 'lists'

    if not api_key:
        return "Error: Please provide the 'api_key' in the request body.", 400
    if not city:
        return "Error: Please provide the 'city' in the request body.", 400
    if not gcs_bucket_name:
        return "Error: Please provide the 'bucket_name' in the request body.", 400

    print(f"Fetching hospitals in {city} and uploading to {gcs_bucket_name}/{folder_name}")

    hospitals = fetch_hospitals(city, api_key)

    if hospitals is None:
        return f"Failed to retrieve hospital data for {city}.", 500

    if not hospitals:
        return f"No hospitals found in {city}.", 200

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    destination_blob_name = f"{folder_name}/hospitals_in_{city.replace(' ', '_')}_{timestamp}.json"
    gcs_uri = upload_to_gcs(gcs_bucket_name, hospitals, destination_blob_name)

    if gcs_uri:
        return f"Successfully fetched and uploaded hospital data for {city} to: {gcs_uri}", 200
    else:
        return f"Failed to upload hospital data for {city} to GCS.", 500

if __name__ == "__main__":
    # Example of how to run locally (for testing)
    # Replace with your actual API key, city, and bucket name
    api_key_local = "YOUR_GOOGLE_PLACES_API_KEY"
    city_local = "Bengaluru"
    bucket_name_local = "YOUR_GCS_BUCKET_NAME"
    folder_local = "test_lists"

    # Simulate a Cloud Function request
    class MockRequest:
        def get_json(self, silent=True):
            return {
                'api_key': api_key_local,
                'city': city_local,
                'bucket_name': bucket_name_local,
                'folder': folder_local
            }

    response = main(MockRequest())
    print(response)