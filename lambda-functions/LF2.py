import json
import boto3
import urllib3
import random
import os

# CONFIGURATION (from environment variables)
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
OS_HOST = os.environ['OPENSEARCH_HOST']
OS_INDEX = os.environ.get('OPENSEARCH_INDEX', 'restaurants')
OS_AUTH = (os.environ['OPENSEARCH_USERNAME'], os.environ['OPENSEARCH_PASSWORD'])
SENDER_EMAIL = os.environ['SENDER_EMAIL']
SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'yelp-restaurants')
# -------------------------------------

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
ses = boto3.client('ses', region_name=AWS_REGION)
sqs = boto3.client('sqs', region_name=AWS_REGION)

http = urllib3.PoolManager()

def lambda_handler(event, context):
    # Actively poll SQS for new messages
    response = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=10, 
        WaitTimeSeconds=2
    )
    
    #If the queue is empty, exit
    if 'Messages' not in response:
        print("No new requests in queue. Waiting for next minute...")
        return
        
    # Process the messages
    for message in response['Messages']:
        receipt_handle = message['ReceiptHandle']
        message_body = json.loads(message['Body'])
        
        cuisine = message_body.get('Cuisine')
        location = message_body.get('Location')
        date = message_body.get('DiningDate', 'today')
        time = message_body.get('DiningTime')
        num_people = message_body.get('NumberOfPeople')
        user_email = message_body.get('Email')
        
        if not cuisine or not user_email:
            print("Missing cuisine or email, skipping.")
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            continue

        cuisine_alias = cuisine.lower()
        if cuisine_alias == 'indian':
            cuisine_alias = 'indpak'
            
        print(f"Looking for {cuisine_alias} restaurants for {user_email}...")
        
        # Query OpenSearch for the Cuisine
        headers = urllib3.util.make_headers(basic_auth=f"{OS_AUTH[0]}:{OS_AUTH[1]}")
        headers['Content-Type'] = 'application/json'
        
        search_url = f"{OS_HOST}/{OS_INDEX}/_search?q=Cuisine:{cuisine_alias}&size=20"
        
        try:
            os_response = http.request('GET', search_url, headers=headers)
            os_data = json.loads(os_response.data.decode('utf-8'))
            hits = os_data.get('hits', {}).get('hits', [])
            
            if not hits:
                send_email(user_email, cuisine, date, time, num_people, [])
            else:
                # Pick random restaurants from the hits (3 recommendations)
                random.shuffle(hits)
                selected_hits = hits[:3]
                
                # Query DynamoDB for the full restaurant details
                table = dynamodb.Table(DYNAMODB_TABLE)
                recommendations = []
                
                for hit in selected_hits:
                    restaurant_id = hit['_source']['RestaurantID']
                    
                    # Fetch from DynamoDB
                    db_response = table.get_item(Key={'Business ID': restaurant_id})
                    if 'Item' in db_response:
                        item = db_response['Item']
                        name = item.get('Name', 'Unknown Name')
                        address = item.get('Address', 'Unknown Address')
                        recommendations.append(f"{name}, located at {address}")
                
                # Send the Email via SES
                send_email(user_email, cuisine, date, time, num_people, recommendations)
                print(f"Successfully processed and emailed recommendations to {user_email}")
            
            # DELETE THE MESSAGE FROM SQS SO WE DON'T EMAIL THEM AGAIN!
            sqs.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )
            print("Message deleted from SQS successfully.")
            
        except Exception as e:
            print("Error processing recommendation:", str(e))

def send_email(recipient, cuisine, date, time, num_people, recommendations):
    if not recommendations:
        text_body = f"Hello! We couldn't find any {cuisine} restaurants in our database right now. Please try another cuisine!"
    else:
        text_body = f"Hello! Here are my {cuisine} restaurant suggestions for {num_people} people, for {date} at {time}:\n\n"
        for i, rec in enumerate(recommendations, 1):
            text_body += f"{i}. {rec}\n"
        text_body += "\nEnjoy your meal!"
        
    try:
        ses.send_email(
            Source=SENDER_EMAIL,
            Destination={'ToAddresses': [recipient]},
            Message={
                'Subject': {'Data': 'Your Dining Recommendations'},
                'Body': {'Text': {'Data': text_body}}
            }
        )
    except Exception as e:
        print("Error sending email:", str(e))
        raise e