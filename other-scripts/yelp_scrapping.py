import requests
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ['YELP_API_KEY']
HEADERS = {'Authorization': f'Bearer {API_KEY}'}
ENDPOINT = 'https://api.yelp.com/v3/businesses/search'

MAX_RETRIES = 5
INITIAL_BACKOFF = 1  # seconds

# required cuisines and location
cuisines = ['chinese', 'italian', 'japanese', 'mexican', 'indian', 'thai']
location = 'Manhattan'

all_restaurants = {}

for cuisine in cuisines:
    print(f"Fetching {cuisine} restaurants...")
    
    # Yelp only returns 50 at a time
    for offset in range(0, 200, 50):
        params = {
            'term': f'{cuisine} restaurants',
            'location': location,
            'limit': 50,
            'offset': offset
        }
        
        for attempt in range(MAX_RETRIES):
            response = requests.get(ENDPOINT, headers=HEADERS, params=params)
            
            if response.status_code == 200:
                data = response.json()
                businesses = data.get('businesses', [])
                
                for biz in businesses:
                    biz_id = biz['id']
                    if biz_id not in all_restaurants:
                        all_restaurants[biz_id] = biz
                break  # success, move on to next request
            elif response.status_code == 429:
                # Rate limited — use Retry-After header if available, otherwise exponential backoff
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    wait = int(retry_after)
                else:
                    wait = INITIAL_BACKOFF * (2 ** attempt)
                print(f"Rate limited on {cuisine} offset {offset}. Retrying in {wait}s (attempt {attempt + 1}/{MAX_RETRIES})...")
                time.sleep(wait)
            else:
                print(f"Error fetching {cuisine} at offset {offset}: {response.status_code}")
                break  # non-retryable error
        else:
            print(f"Failed to fetch {cuisine} at offset {offset} after {MAX_RETRIES} retries.")

print(f"Total unique restaurants collected: {len(all_restaurants)}")

# Save the raw data to a local JSON file to use in DynamoDB
with open('raw_yelp_data.json', 'w') as f:
    json.dump(list(all_restaurants.values()), f, indent=4)
    
print("Data saved to raw_yelp_data.json!")