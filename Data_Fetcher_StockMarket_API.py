import requests
import json
from confluent_kafka import Producer


def fetch_stock_data():
    url = "https://latest-stock-price.p.rapidapi.com/any"

    
    headers = {
        "X-RapidAPI-Key": "e8454ac2femsh934926ecdbb3cfbp1b879fjsn7cb502b129e3",
        "X-RapidAPI-Host": "latest-stock-price.p.rapidapi.com"
    }

    try:
        response = requests.get(url, headers=headers)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            data = response.json()
            print("API request successful")

            return data
        else:
            print(f"API request failed with status code {response.status_code}")
            print("Response:", response.text),
            return("Response:", response.text)

    except Exception as e:
        print("An error occurred:", str(e))

def publish_to_kafka(topic, message):
    #print('Inside Publish to kafka function', topic, message)  # for test
    kafka_brokers = '127.0.0.1:9092'    

    producer_conf = {'bootstrap.servers': kafka_brokers}
    
    producer = Producer(producer_conf)

    try:
        producer.produce(topic, value=message)
        producer.flush()
        print(f"Sample message '{message}' sent to topic '{topic}' successfully.")
    except Exception as e:
        print(f"Error: {e}")

stock_data = fetch_stock_data()
#print('The data is :', stock_data)  #For Testing
    
for record in stock_data:

    identifier = record['identifier']

    if identifier == 'KOTAKBANKEQN' or identifier == 'HDFCBANKEQN' or identifier == 'PFCEQN' or identifier == 'ICICIPRULIEQN':
        publish_to_kafka('Financial', json.dumps(record))
    elif identifier == 'BPCLEQN' or identifier == 'ONGCEQN' or identifier == 'ADANIENSOLEQN' or identifier == 'GAILEQN':
        publish_to_kafka('OilGas', json.dumps(record))
    elif identifier == 'CIPLAEQN' or identifier == 'AUROPHARMAEQN' or identifier == 'ZYDUSLIFEEQN' or identifier == 'ABBOTINDIAEQN':
        publish_to_kafka('Healthcare', json.dumps(record))
    elif identifier == 'VBLEQN' or identifier == 'NESTLEINDEQN' or identifier == 'UBLEQN' or identifier == 'JUBLFOODEQN':
        publish_to_kafka('FMCG', json.dumps(record))
    
#test_var=lambda_handler()
#print(test_var)    
