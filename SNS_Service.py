import json
import boto3
from decimal import Decimal

# Initialize AWS SNS client
sns = boto3.client('sns')

# SNS Topic ARN
topic_arn = "aarn:aws:sns:ap-south-1:278793623371:StockMarketTopic_Healthcare" # Change arn of every topic while deploy and test code in Lambda function.

# DynamoDB Table Name
table_name = "Stocks_Healthcare" # Change table name of every topic while deploy and test code in Lambda function.

# Function to fetch data from DynamoDB
def fetch_data_from_dynamodb():
    dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
    table = dynamodb.Table(table_name)
    
    # Example: Fetching data from DynamoDB table
    response = table.scan()
    items = response['Items']
    
    return items

# Fetch data from DynamoDB
message_data = fetch_data_from_dynamodb()

# Convert Decimal objects to float for serialization
def convert_decimals_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, list):
        return [convert_decimals_to_float(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_decimals_to_float(value) for key, value in obj.items()}
    return obj

# Format message data
def format_message_data(data):
    formatted_data = ""
    for item in data:
        for key, value in item.items():
            formatted_data += f"{key}: {value}\n"
        formatted_data += "\n"  # Add a newline after each item
    return formatted_data

# Publish message to SNS topic
sns.publish(
    TopicArn=topic_arn,
    Message=format_message_data(message_data),
    Subject='Stock Market Data of Healthcare Companies' # Change subject msg of every topic while deploy and test code in Lambda function.
)

print('Message published to SNS topic')

# Lambda handler function
def lambda_handler(event, context):
    return {
        'statusCode': 200,
        'body': json.dumps('SNS message processed successfully!')
    }
