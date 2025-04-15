import argparse
import logging
import json
import random
import time
import os
from datetime import date, datetime
from faker import Faker
from google.cloud import storage

# Set up logging for progress and error reporting.
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
fake = Faker()

# Attempt to import googlemaps for real data fetching.
try:
    import googlemaps
except ImportError:
    googlemaps = None
    logging.warning("googlemaps library not installed. Real data will not be available.")

# Optionally import API key from environment variable.
CONFIG_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY")

# Constants
REAL_REVIEWS_LIMIT = 5         # Maximum number of real reviews fetched per hospital
MIN_REVIEWS_PER_HOSPITAL = 100  # Ensure each hospital has at least 100 reviews

# -------------------------------
# Enhanced Templates and Adjectives
# -------------------------------

# Positive templates (20 variants)
positive_templates = [
    "The service was {adjective} and the staff were {adjective2}. I felt {adjective3} with the high quality care provided. The experience was truly unforgettable and exceeded all my expectations.",
    "I had an extremely {adjective} visit where the doctors, nurses, and every team member were {adjective2}. Their professionalism made me feel {adjective3} throughout my entire stay.",
    "My experience was absolutely {adjective} as the hospital was impeccably organized with {adjective2} staff. I was left feeling {adjective3} and grateful for the outstanding medical care.",
    "This hospital provided {adjective} service from beginning to end. The {adjective2} team ensured that I felt {adjective3} with every procedure and consultation.",
    "I was overwhelmed by the {adjective} level of care, thanks to the {adjective2} staff. Their dedication made me feel {adjective3} and very well taken care of.",
    "The entire service was {adjective} from start to finish. The {adjective2} professionals left me feeling {adjective3} and appreciative of their expertise.",
    "I had an {adjective} experience in every sense. The hospital’s {adjective2} staff ensured I felt {adjective3} and comfortable during my visit.",
    "From the moment I arrived, the {adjective} service and {adjective2} attitude of the team made me feel {adjective3} and confident in the care provided.",
    "The care was remarkably {adjective}, with {adjective2} professionals who made sure I felt {adjective3} and respected throughout my treatment.",
    "I experienced {adjective} service and {adjective2} warmth from every staff member. I left feeling {adjective3} and truly valued.",
    "The hospital boasts {adjective} facilities and a {adjective2} team. I felt {adjective3} due to the unparalleled care and attention given to me.",
    "Every step of the care was {adjective}. The {adjective2} team ensured a smooth experience, leaving me feeling {adjective3} and reassured.",
    "I was genuinely impressed by the {adjective} service and {adjective2} professionalism. It made me feel {adjective3} and optimistic about my recovery.",
    "The environment was {adjective} and the {adjective2} care team left me feeling {adjective3} throughout the entire process.",
    "I received {adjective} treatment that was second to none. The {adjective2} staff’s attention to detail made me feel {adjective3} and cared for.",
    "The dedication at this hospital is {adjective}. Their {adjective2} approach provided me with {adjective3} care in an incredibly supportive environment.",
    "I felt {adjective3} thanks to the {adjective2} care provided by a truly {adjective} hospital. The services here are outstanding in every way.",
    "The service was consistently {adjective} and the staff's {adjective2} commitment made me feel {adjective3}. Their care went well beyond my expectations.",
    "I was thoroughly impressed by the {adjective} environment and the {adjective2} professionals. I left feeling {adjective3} and ready to recommend this hospital.",
    "With {adjective} care and {adjective2} attention to every detail, I felt {adjective3} during every moment of my visit."
]

# Neutral templates (20 variants)
neutral_templates = [
    "The hospital was {adjective} overall, although there were some aspects that left me feeling {adjective2}. My experience was moderately {adjective3} and could use some improvement.",
    "My visit was {adjective}, with a few {adjective2} moments that made the service feel rather {adjective3}. It was neither exceptionally good nor bad.",
    "The service provided was {adjective}, and while some elements were {adjective2}, the overall experience felt {adjective3} without leaving a lasting impression.",
    "I found the hospital to be {adjective}, although there were some {adjective2} delays that resulted in a {adjective3} experience.",
    "Overall, my experience was {adjective}. The staff was {adjective2}, but certain procedures felt {adjective3} and lacked efficiency.",
    "The visit was {adjective} but somewhat unpredictable; while some aspects were {adjective2}, others felt {adjective3}.",
    "It was a {adjective} hospital environment. The service was {adjective2} on occasion, making the experience generally {adjective3}.",
    "I would describe the hospital as {adjective}, with a mix of {adjective2} strengths and {adjective3} delays affecting my overall experience.",
    "The hospital was {adjective}, and although some staff acted {adjective2}, the overall service was a bit {adjective3}.",
    "While the experience was {adjective}, there were moments that felt {adjective2}. In total, the care was somewhat {adjective3}.",
    "My experience was fairly {adjective}, with periodic {adjective2} service that made the experience come off as {adjective3}.",
    "The hospital maintained a {adjective} ambiance; however, slight {adjective2} inefficiencies resulted in a rather {adjective3} service.",
    "I found the overall service to be {adjective} with some {adjective2} moments that made it feel somewhat {adjective3} during my visit.",
    "It was an average, {adjective} experience with parts of the service being notably {adjective2}, leading to a predominantly {adjective3} impression.",
    "The care was {adjective} throughout, with fluctuations between {adjective2} attentiveness and {adjective3} pacing in the service.",
    "While the environment was {adjective}, I occasionally noted {adjective2} shortcomings that rendered the overall experience {adjective3}.",
    "Overall, the hospital service was {adjective}. Some parts of the experience were {adjective2}, which made it feel rather {adjective3}.",
    "I experienced a {adjective} visit that alternated between {adjective2} moments and a generally {adjective3} level of service.",
    "The hospital was {adjective} in its service. At times, the staff was {adjective2}, but overall the experience felt {adjective3}.",
    "There were elements of both {adjective} and {adjective2} service during my visit, leaving the overall experience as fairly {adjective3}."
]

# Negative templates (20 variants)
negative_templates = [
    "The service was {adjective} and {adjective2}. The staff were {adjective3}, and I felt very {adjective4} as a result of the poor care.",
    "My experience was extremely {adjective}. The hospital was {adjective2} overall, and the service was {adjective3}, leaving me feeling {adjective4} and disappointed.",
    "I was very {adjective} with my experience. The facility was {adjective2} and the service was {adjective3}, which left me feeling {adjective4} throughout my visit.",
    "The hospital proved to be {adjective}. The care was {adjective2} and insufficient, making the overall experience {adjective3} and leaving me {adjective4}.",
    "I felt utterly {adjective} during my stay. The staff was {adjective2} and the service was {adjective3}, so much so that I felt {adjective4}.",
    "The service was undeniably {adjective} with {adjective2} shortcomings. I was left feeling {adjective3} and truly {adjective4}.",
    "I had a {adjective} experience. The hospital was {adjective2} and the level of care was {adjective3}, leaving me feeling very {adjective4}.",
    "The overall service was {adjective} and completely {adjective2}. I felt {adjective3} and ultimately {adjective4} due to the inadequate treatment.",
    "My experience was {adjective}. The care was {adjective2}, and the staff behaved in a {adjective3} way, which ultimately made me feel {adjective4}.",
    "The hospital was {adjective} and the service {adjective2}. I felt extremely {adjective3} and ultimately {adjective4} due to the inadequate treatment.",
    "I was extremely {adjective} with the way I was treated. The service was {adjective2} and the facility felt {adjective3}, leaving me {adjective4}.",
    "The level of care was {adjective}. With {adjective2} service and {adjective3} staff, I felt profoundly {adjective4} during my visit.",
    "The overall service was {adjective}; the staff were {adjective2} and the approach was {adjective3}, making me feel very {adjective4}.",
    "Unfortunately, my experience was {adjective}. The hospital was {adjective2} and the service {adjective3}, which made me feel {adjective4} throughout.",
    "I encountered {adjective} service at the hospital. The staff were quite {adjective2}, and the care was {adjective3}, leaving me feeling {adjective4}.",
    "The experience was undeniably {adjective} and the staff seemed {adjective2}. With {adjective3} treatment, I ended up feeling {adjective4}.",
    "The facility provided {adjective} service, where the care was {adjective2} and the staff acted {adjective3}, causing me to feel {adjective4}.",
    "I felt very {adjective} during my stay. The hospital care was {adjective2} and overall service {adjective3}, making the experience extremely {adjective4}.",
    "The service was exceptionally {adjective} and the staff {adjective2}; however, the treatment was {adjective3} and left me feeling {adjective4}.",
    "Overall, my experience was {adjective}. With {adjective2} care and an approach that was {adjective3}, I felt undeniably {adjective4}."
]

# Enhanced adjectives for each rating category
adjectives_positive = ["excellent", "outstanding", "superb", "wonderful", "amazing", "incredible", "fantastic", "perfect",
                        "impressive", "phenomenal", "great", "marvelous", "top-notch", "super", "unbelievable",
                        "exceptional", "spectacular", "first-rate", "awesome", "remarkable"]

adjectives_neutral = ["mediocre", "average", "ordinary", "satisfactory", "standard", "fair", "acceptable", "decent",
                        "unremarkable", "typical", "sufficient", "okay", "fine", "passable", "middling", "run-of-the-mill",
                        "plain", "undistinguished", "unexceptional", "moderate"]

adjectives_negative = ["poor", "terrible", "disappointing", "unsatisfactory", "subpar", "abysmal", "horrible", "bad",
                        "awful", "atrocious", "mediocre", "dismal", "frustrating", "unacceptable", "lousy", "unpleasant",
                        "distressing", "horrendous", "unfortunate", "regrettable"]

def generate_synthetic_review(rating):
    """
    Generate a synthetic review entry with diverse language based on the rating.
    """
    if rating >= 4:
        template = random.choice(positive_templates)
        adjective = random.choice(adjectives_positive)
        adjective2 = random.choice(adjectives_positive)
        adjective3 = random.choice(adjectives_positive)
        adjective4 = random.choice(adjectives_positive)
    elif rating == 3:
        template = random.choice(neutral_templates)
        adjective = random.choice(adjectives_neutral)
        adjective2 = random.choice(adjectives_neutral)
        adjective3 = random.choice(adjectives_neutral)
        adjective4 = random.choice(adjectives_neutral)
    else:
        template = random.choice(negative_templates)
        adjective = random.choice(adjectives_negative)
        adjective2 = random.choice(adjectives_negative)
        adjective3 = random.choice(adjectives_negative)
        adjective4 = random.choice(adjectives_negative)

    review_text = template.format(adjective=adjective, adjective2=adjective2, adjective3=adjective3, adjective4=adjective4)
    review_date = fake.date_between_dates(date_start=date(2020, 1, 1), date_end=date(2024, 12, 31))
    return {
        "author_name": fake.name(),
        "author_url": fake.url(),
        "language": "en",
        "original_language": "en",
        "profile_photo_url": fake.image_url(),
        "rating": rating,
        "relative_time_description": f"{random.randint(1, 12)} months ago",
        "text": review_text,
        "time": int(time.mktime(review_date.timetuple())),
        "translated": False,
        "review_type": "fake",
        "date": review_date.strftime("%Y-%m-%d")
    }

def ensure_minimum_reviews(hospital, min_count=MIN_REVIEWS_PER_HOSPITAL):
    """
    Ensure the hospital object has at least `min_count` reviews by appending synthetic reviews.
    """
    while len(hospital.get("reviews", [])) < min_count:
        hospital["reviews"].append(generate_synthetic_review(random.randint(1, 5)))

def fetch_real_hospitals(city, api_key):
    """
    Fetch real hospital data from the Google Places API.
    Each hospital will have up to REAL_REVIEWS_LIMIT real reviews; synthetic reviews are appended if needed.
    Returns a list of hospital dictionaries with keys matching the API response format.
    """
    if googlemaps is None:
        logging.error("googlemaps library is not installed. Cannot fetch real data.")
        return []

    gmaps = googlemaps.Client(key=api_key)
    hospitals = []
    try:
        logging.info(f"Fetching real hospital data for '{city}' using Google Places API.")
        places_result = gmaps.places(query=f"hospital in {city}", type="hospital")
        results = places_result.get("results", [])
        for result in results:
            hospital = {}
            hospital["name"] = result.get("name")
            hospital["formatted_address"] = result.get("formatted_address", "N/A")
            hospital["place_id"] = result.get("place_id", None)
            hospital["rating"] = result.get("rating", None)
            hospital["user_ratings_total"] = result.get("user_ratings_total", None)
            hospital["reviews"] = []
            try:
                details = gmaps.place(place_id=result.get("place_id"), fields=["reviews"])
                detail_result = details.get("result", {})
                reviews = detail_result.get("reviews", [])[:REAL_REVIEWS_LIMIT]
                for review in reviews:
                    review_entry = {
                        "author_name": review.get("author_name"),
                        "author_url": review.get("author_url"),
                        "language": review.get("language"),
                        "original_language": review.get("original_language", "en"),
                        "profile_photo_url": review.get("profile_photo_url"),
                        "rating": review.get("rating"),
                        "relative_time_description": review.get("relative_time_description"),
                        "text": review.get("text"),
                        "time": review.get("time"),
                        "translated": review.get("translated", False),
                        "review_type": "real",
                        "date": datetime.fromtimestamp(review.get("time")).strftime("%Y-%m-%d") if review.get("time") else "N/A"
                    }
                    hospital["reviews"].append(review_entry)
            except Exception as e:
                logging.error(f"Error fetching details for hospital '{result.get('name')}': {e}")
                continue
            ensure_minimum_reviews(hospital, MIN_REVIEWS_PER_HOSPITAL)
            hospitals.append(hospital)
            time.sleep(1)
    except Exception as e:
        logging.error(f"Error fetching hospital data: {e}")
    logging.info(f"Fetched real data for {len(hospitals)} hospitals.")
    return hospitals

def generate_synthetic_hospital(city):
    """
    Generate a synthetic hospital object with fake data that mimics the API response keys.
    """
    hospital = {}
    hospital_types = ["General Hospital", "Medical Center", "Health Clinic", "Regional Hospital", "Community Hospital"]
    hospital["name"] = f"{city} {random.choice(hospital_types)}"
    hospital["formatted_address"] = fake.address().replace("\n", ", ")
    hospital["place_id"] = None  # Synthetic hospital; no real place_id.
    hospital["rating"] = round(random.uniform(1, 5), 1)
    hospital["user_ratings_total"] = random.randint(50, 5000)
    hospital["reviews"] = []
    for _ in range(MIN_REVIEWS_PER_HOSPITAL):
        hospital["reviews"].append(generate_synthetic_review(random.randint(1, 5)))
    return hospital

def generate_synthetic_hospitals(city, count):
    """
    Generate a list of synthetic hospital objects.
    """
    hospitals = []
    for _ in range(count):
        hospital = generate_synthetic_hospital(city)
        hospitals.append(hospital)
    return hospitals

def upload_to_gcs(bucket_name, data, output_filename, folder_name=None):
    """Uploads the combined JSON data to Google Cloud Storage, optionally within a folder."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob_name = output_filename
        if folder_name:
            blob_name = os.path.join(folder_name, output_filename)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(json.dumps({"disclaimer": (
                "For educational use only. Real data fetched via the Google Places API (if present) "
                "may not reflect actual patient experiences. All synthetic data is generated for academic research purposes only."
            ),
            "data": data
        }, indent=4, ensure_ascii=False), 'application/json')
        logging.info(f"Combined hospital review data uploaded to gs://{bucket_name}/{blob_name}")
        return f"gs://{bucket_name}/{blob_name}"
    except Exception as e:
        logging.error(f"Error uploading to GCS: {e}")
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
    city = request_json.get('city')
    api_key = request_json.get('api_key') or CONFIG_API_KEY
    bucket_name = request_json.get('bucket_name')
    folder_name = request_json.get('folder_name') # Optional folder name
    # Construct output filename based on the city
    output_filename = f"{city.lower().replace(' ', '_')}_hospital_reviews.json"

    if not city:
        return "Error: Please provide the 'city' in the request body.", 400
    if not bucket_name:
        return "Error: Please provide the 'bucket_name' in the request body.", 400

    combined_data = []
    if api_key:
        logging.info("API key provided. Fetching real hospital data. For educational use only.")
        real_data = fetch_real_hospitals(city, api_key)
        combined_data.extend(real_data)
    else:
        logging.info("No API key provided; generating synthetic hospital data.")
        synthetic_data = generate_synthetic_hospitals(city, 5)
        combined_data.extend(synthetic_data)

    for hospital in combined_data:
        ensure_minimum_reviews(hospital, MIN_REVIEWS_PER_HOSPITAL)

    total_hospitals = len(combined_data)
    total_reviews = sum(len(h["reviews"]) for h in combined_data)
    logging.info(f"Total hospitals: {total_hospitals}, Total reviews: {total_reviews}")

    gcs_uri = upload_to_gcs(bucket_name, combined_data, output_filename, folder_name)

    if gcs_uri:
        return f"Successfully generated and uploaded hospital review data to {gcs_uri}", 200
    else:
        return "Error: Failed to upload data to Google Cloud Storage.", 500

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate combined real and synthetic hospital review data and optionally upload to GCS."
    )
    parser.add_argument("city", help="City name (e.g., 'Delhi', 'Boston')")
    parser.add_argument("--api_key", help="Optional Google Places API key for real data", default=None)
    parser.add_argument("--bucket_name", help="GCP Cloud Storage bucket name to upload the JSON file", default=None)
    parser.add_argument("--folder_name", help="Optional folder name within the bucket", default=None)
    args = parser.parse_args()

    city = args.city
    api_key = args.api_key or CONFIG_API_KEY
    bucket_name = args.bucket_name
    folder_name = args.folder_name
    output_filename = f"{city.lower().replace(' ', '_')}_hospital_reviews.json"

    combined_data = []
    if api_key:
        logging.info("API key provided. Fetching real hospital data (for local execution).")
        real_data = fetch_real_hospitals(city, api_key)
        combined_data.extend(real_data)
    else:
        logging.info("No API key provided; generating synthetic hospital data (for local execution).")
        synthetic_data = generate_synthetic_hospitals(city, 5)
        combined_data.extend(synthetic_data)

    for hospital in combined_data:
        ensure_minimum_reviews(hospital)

    total_hospitals = len(combined_data)
    total_reviews = sum(len(h["reviews"]) for h in combined_data)
    logging.info(f"Total hospitals: {total_hospitals}, Total reviews: {total_reviews} (local generation)")

    output_data = {
        "disclaimer": (
            "For educational use only. Real data fetched via the Google Places API (if present) "
            "may not reflect actual patient experiences. All synthetic data is generated for academic research purposes only."
        ),
        "data": combined_data
    }

    if bucket_name:
        upload_to_gcs(bucket_name, combined_data, output_filename, folder_name)
    else:
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=4, ensure_ascii=False)
        logging.info(f"Combined hospital review data saved locally to {output_filename}")