import json
import boto3
import datetime
import os

sqs = boto3.client('sqs')
QUEUE_URL = os.environ['SQS_QUEUE_URL']
HISTORY_TABLE = os.environ.get('DYNAMODB_HISTORY_TABLE', 'UserHistory')

# VALIDATION LOGIC
def validate_slots(slots):
    # Validate Cuisine
    if slots.get('Cuisine') and slots['Cuisine'].get('value'):
        cuisine = slots['Cuisine']['value'].get('interpretedValue', '').lower()
        valid_cuisines = ['indian', 'italian', 'japanese', 'mexican', 'chinese', 'thai']
        if cuisine not in valid_cuisines:
            return {
                'isValid': False,
                'violatedSlot': 'Cuisine',
                'message': f"Sorry, we only support {', '.join(valid_cuisines)}. Which of those would you like?"
            }

    # Validate Number of People
    if slots.get('NumberOfPeople') and slots['NumberOfPeople'].get('value'):
        try:
            num_people = int(slots['NumberOfPeople']['value'].get('interpretedValue', 0))
            if num_people <= 0 or num_people > 20:
                return {
                    'isValid': False,
                    'violatedSlot': 'NumberOfPeople',
                    'message': "Please enter a valid party size between 1 and 20."
                }
        except ValueError:
            return {
                'isValid': False,
                'violatedSlot': 'NumberOfPeople',
                'message': "Please enter a valid number for your party size."
            }
            
    # Validate Location
    if slots.get('Location') and slots['Location'].get('value'):
        location = slots['Location']['value'].get('interpretedValue', '').lower()
        # Acceptable variations of NYC
        valid_locations = ['new york', 'new york city', 'nyc', 'manhattan', 'ny']
        
        if location not in valid_locations:
            return {
                'isValid': False,
                'violatedSlot': 'Location',
                'message': "I'm sorry, my database currently only has restaurants in New York City. Could you please say 'New York' or 'Manhattan'?"
            }

    # Validate Date
    if slots.get('DiningDate') and slots['DiningDate'].get('value'):
        date_str = slots['DiningDate']['value'].get('interpretedValue')
        try:
            # Parse Lex's date (YYYY-MM-DD)
            dining_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # Get today's date in New York (UTC - 5 hours)
            nyc_now = datetime.datetime.utcnow() - datetime.timedelta(hours=5)
            today_date = nyc_now.date()
            
            if dining_date < today_date:
                return {
                    'isValid': False,
                    'violatedSlot': 'DiningDate',
                    'message': "You can't book a restaurant in the past! Please provide today's date or a future date."
                }
        except ValueError:
            return {
                'isValid': False,
                'violatedSlot': 'DiningDate',
                'message': "I didn't quite catch that date. What day would you like to dine?"
            }

    # Validate Time & Check for AM/PM
    if slots.get('DiningTime') and slots['DiningTime'].get('value'):
        time_str = slots['DiningTime']['value'].get('interpretedValue')
        original_val = slots['DiningTime']['value'].get('originalValue', '').lower()
        
        # Check if the user just typed a number like "5" or "10" without AM/PM
        if original_val.strip().isdigit():
            return {
                'isValid': False,
                'violatedSlot': 'DiningTime',
                'message': f"You said {original_val}. Did you mean {original_val} AM or {original_val} PM?"
            }
            
        try:
            # Parse Lex's 24-hour time format (HH:MM)
            hour, minute = map(int, time_str.split(':'))
            dining_time = datetime.time(hour, minute)
            
            # If they are booking for TODAY, make sure the time is in the future
            if slots.get('DiningDate') and slots['DiningDate'].get('value'):
                date_str = slots['DiningDate']['value'].get('interpretedValue')
                nyc_now = datetime.datetime.utcnow() - datetime.timedelta(hours=5)
                
                if date_str == str(nyc_now.date()):
                    if dining_time < nyc_now.time():
                        return {
                            'isValid': False,
                            'violatedSlot': 'DiningTime',
                            'message': "That time has already passed! What time later today would you like to eat?"
                        }
        except ValueError:
            return {
                'isValid': False,
                'violatedSlot': 'DiningTime',
                'message': "I didn't understand that time. For example, you can say '7 PM'."
            }

    return {'isValid': True}

def lambda_handler(event, context):
    intent_name = event['sessionState']['intent']['name']
    session_attributes = event['sessionState'].get('sessionAttributes', {})
    user_email = session_attributes.get('email')
    
    if intent_name == 'GreetingIntent':
        if user_email:
            # Check DynamoDB 'UserHistory' table
            history_table = boto3.resource('dynamodb').Table(HISTORY_TABLE)
            response = history_table.get_item(Key={'Email': user_email}) 
            
            if 'Item' in response:
                last_cuisine = response['Item']['LastCuisine']
                msg = f"Welcome back! Last time you looked for {last_cuisine}. Want to do that again or try something new?"
                return close_dialog(event, msg)
        
        return close_dialog(event, "Hi there, how can I help you today?")
    elif intent_name == 'ThankYouIntent':
        return close_dialog(event, "You're welcome!")
    elif intent_name == 'DiningSuggestionsIntent':
        return handle_dining_suggestions(event)
    elif intent_name == 'RepeatSearchIntent':
        return handle_repeat_search(event)
        
    raise Exception(f"Intent {intent_name} not supported")

def handle_dining_suggestions(event):
    slots = event['sessionState']['intent']['slots']
    invocation_source = event['invocationSource']
    
    if invocation_source == 'DialogCodeHook':
        # Check if the slots are valid
        validation_result = validate_slots(slots)
        
        if not validation_result['isValid']:
            # If invalid, ask for THAT specific slot again
            return elicit_slot(
                event, 
                validation_result['violatedSlot'], 
                validation_result['message']
            )
            
        # If everything looks good
        return delegate_dialog(event)
        
    elif invocation_source == 'FulfillmentCodeHook':
        try:
            location = slots.get('Location', {}).get('value', {}).get('interpretedValue', 'Unknown')
            cuisine = slots.get('Cuisine', {}).get('value', {}).get('interpretedValue', 'Unknown')
            date = slots.get('DiningDate', {}).get('value', {}).get('interpretedValue', 'Unknown')
            time = slots.get('DiningTime', {}).get('value', {}).get('interpretedValue', 'Unknown')
            num_people = slots.get('NumberOfPeople', {}).get('value', {}).get('interpretedValue', 'Unknown')
            email = slots.get('Email', {}).get('value', {}).get('interpretedValue', 'Unknown')
            
            sqs_message = {
                "Location": location, "Cuisine": cuisine, "DiningTime": time, "DiningDate": date,
                "NumberOfPeople": num_people, "Email": email
            }
            
            sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(sqs_message))
            
            # Initialize DynamoDB client and save the user's last search history
            dynamo_history = boto3.resource('dynamodb').Table(HISTORY_TABLE)
            dynamo_history.put_item(
                Item={
                    'Email': email,
                    'LastCuisine': cuisine,
                    'LastLocation': location
                }
            )
            print(f"Saved history for {email}")

            return close_dialog(event, f"I have received your request for {cuisine} food and will notify you at {email} shortly.")
        except Exception as e:
            print(f"Error extracting slots: {e}")
            return close_dialog(event, "I'm sorry, I missed some of that information. Could we start over?")

def handle_repeat_search(event):
    slots = event['sessionState']['intent']['slots']
    invocation_source = event['invocationSource']
    user_email = event['sessionState'].get('sessionAttributes', {}).get('email')

    # Fetch past favorites from DynamoDB
    history_table = boto3.resource('dynamodb').Table(HISTORY_TABLE)
    response = history_table.get_item(Key={'Email': user_email})
    cuisine = response['Item']['LastCuisine']
    location = response['Item']['LastLocation']

    if invocation_source == 'DialogCodeHook':
        # Ask the user for the Date, Time, and Number of People
        return delegate_dialog(event)

    elif invocation_source == 'FulfillmentCodeHook':
        date = slots.get('DiningDate', {}).get('value', {}).get('interpretedValue', 'Unknown')
        time = slots.get('DiningTime', {}).get('value', {}).get('interpretedValue', 'Unknown')
        num_people = slots.get('NumberOfPeople', {}).get('value', {}).get('interpretedValue', 'Unknown')

        # Create the SQS message combining old favorites + new time
        sqs_message = {
            "Location": location, "Cuisine": cuisine, 
            "DiningDate": date, "DiningTime": time,
            "NumberOfPeople": num_people, "Email": user_email
        }
        
        # Send to SQS
        sqs = boto3.client('sqs')
        sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(sqs_message))

        return close_dialog(event, f"Perfect! I've put in a request for {cuisine} food in {location} for {num_people} people. I will email you at {user_email} shortly!")
    
# HELPER FUNCTIONS
def close_dialog(event, message):
    return {
        "sessionState": {
            "dialogAction": {"type": "Close"},
            "intent": {
                "name": event['sessionState']['intent']['name'],
                "state": "Fulfilled"
            }
        },
        "messages": [{"contentType": "PlainText", "content": message}]
    }

def delegate_dialog(event):
    return {
        "sessionState": {
            "dialogAction": {"type": "Delegate"},
            "intent": event['sessionState']['intent']
        }
    }

# Ask the user for a specific slot again
def elicit_slot(event, violated_slot, message):
    return {
        "sessionState": {
            "dialogAction": {
                "type": "ElicitSlot",
                "slotToElicit": violated_slot
            },
            "intent": event['sessionState']['intent']
        },
        "messages": [{"contentType": "PlainText", "content": message}]
    }

