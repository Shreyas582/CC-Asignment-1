import boto3
import json
import datetime
import os
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()

# Initialize the DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
table = dynamodb.Table(os.environ.get('DYNAMODB_TABLE', 'yelp-restaurants'))

# Helper function to convert floats to Decimals
def float_to_decimal(data):
    if isinstance(data, float):
        return Decimal(str(data))
    if isinstance(data, dict):
        return {k: float_to_decimal(v) for k, v in data.items()}
    if isinstance(data, list):
        return [float_to_decimal(v) for v in data]
    return data

def upload_data():
    with open('raw_yelp_data.json', 'r') as file:
        restaurants = json.load(file)
        
    print(f"Loaded {len(restaurants)} restaurants. Starting upload...")
    
    count = 0
    # Process and upload each restaurant
    for r in restaurants:
        try:
            # Format the address as a single string
            address = ", ".join(r.get('location', {}).get('display_address', []))
            
            item = {
                'Business ID': r['id'], # Partition key
                'Name': r.get('name', 'Unknown'),
                'Address': address,
                'Coordinates': float_to_decimal(r.get('coordinates', {})),
                'Number of Reviews': r.get('review_count', 0),
                'Rating': float_to_decimal(r.get('rating', 0)),
                'Zip Code': r.get('location', {}).get('zip_code', 'Unknown'),
                'insertedAtTimestamp': str(datetime.datetime.now())
            }
            
            # Upload to DynamoDB
            table.put_item(Item=item)
            count += 1
            
            # Print progress
            if count % 100 == 0:
                print(f"Uploaded {count} restaurants...")
                
        except Exception as e:
            print(f"Error uploading {r.get('id')}: {str(e)}")
            
    print(f"Success! Uploaded a total of {count} restaurants to DynamoDB.")

if __name__ == '__main__':
    upload_data()