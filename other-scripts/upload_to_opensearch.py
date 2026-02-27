from opensearchpy import OpenSearch, RequestsHttpConnection
from dotenv import load_dotenv
import json
import os

load_dotenv()

# Configuration
host = os.environ['OPENSEARCH_HOST']
region = os.environ.get('AWS_REGION', 'us-east-1')
auth = (os.environ['OPENSEARCH_USERNAME'], os.environ['OPENSEARCH_PASSWORD'])

# Create the OpenSearch client
client = OpenSearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

def upload_to_opensearch():
    with open('raw_yelp_data.json', 'r') as file:
        restaurants = json.load(file)
        
    print(f"Loaded {len(restaurants)} restaurants. Starting upload to OpenSearch...")
    
    # create the restaurants index
    index_name = 'restaurants'
    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name)
        print(f"Created index: {index_name}")

    count = 0
    for r in restaurants:
        try:
            document = {
                'RestaurantID': r['id'],
                'Cuisine': r['categories'][0]['alias'] if r.get('categories') else 'unknown' 
            }
            document['type'] = 'Restaurant'
            
            # Upload the document
            response = client.index(
                index=index_name,
                body=document,
                id=r['id'],
                refresh=True
            )
            count += 1
            if count % 100 == 0:
                print(f"Uploaded {count} records to OpenSearch...")
                
        except Exception as e:
            print(f"Error uploading {r['id']}: {e}")
            
    print(f"Success! Uploaded {count} records to OpenSearch.")

if __name__ == '__main__':
    upload_to_opensearch()