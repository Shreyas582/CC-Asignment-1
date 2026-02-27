import json
import boto3
import uuid
import os

# Initialize the Lex V2 client
lex_client = boto3.client('lexv2-runtime')

def lambda_handler(event, context):
    BOT_ID = os.environ['LEX_BOT_ID']
    BOT_ALIAS_ID = os.environ['LEX_BOT_ALIAS_ID']
    LOCALE_ID = os.environ.get('LEX_LOCALE_ID', 'en_US')
    
    try:
        # {"messages": [{"unstructured": {"text": "hello"}}]}
        body = json.loads(event.get('body', '{}'))
        user_message = body['messages'][0]['unstructured']['text']
        
        user_email = body.get('userEmail', 'sss10093@nyu.edu')

        # Send to the Lex chatbot and wait for the response
        try:
            lex_response = lex_client.recognize_text(
                botId=BOT_ID,
                botAliasId=BOT_ALIAS_ID,
                localeId=LOCALE_ID,
                sessionId=user_email.replace('@', '-'),
                text=user_message,
                sessionState={
                    'sessionAttributes': {
                        'email': user_email
                    }
                }
            )
        except Exception as e:
            print("Error calling Lex:", e)
            raise e
        
        # Extract the bot's reply from the Lex response object
        bot_reply = "I didn't quite catch that."
        if 'messages' in lex_response and len(lex_response['messages']) > 0:
            bot_reply = lex_response['messages'][0]['content']
            
        # Send back the response from Lex as the API response
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'messages': [
                    {
                        'type': 'unstructured',
                        'unstructured': {
                            'text': bot_reply
                        }
                    }
                ]
            })
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Something went wrong with the chatbot API.')
        }