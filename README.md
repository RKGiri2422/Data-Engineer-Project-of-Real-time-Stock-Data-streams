# Data-Engineer-Project-of-Real-time-Stock-Data-streams
Stock Market Data Streaming and Notification System
This project is designed to fetch stock market data from an API, publish the data to Apache Kafka topics, consume the data from Kafka, store it in DynamoDB tables, and finally, send notifications to an SNS topic with the stored data.

**Project Overview**
The project consists of three main components:
Data Fetcher: This component fetches stock market data from an external API (in this case, "https://latest-stock-price.p.rapidapi.com/any") and publishes the data to respective Kafka topics based on the stock category (Financial, OilGas, Healthcare, and FMCG).
Data Consumer: This component consumes data from the Kafka topics, processes the data, and stores it in separate DynamoDB tables for each stock category.
SNS Service: This component fetches data from the DynamoDB tables and publishes it as a notification to respective SNS topics for each stock category.

**Prerequisites**
Before running the project, you'll need the following:
Python 3.x
Apache Kafka (You can use Conduktor Desktop or any other Kafka distribution)
AWS CLI configured with appropriate credentials
DynamoDB and SNS services set up in your AWS account

**Project Structure**
The project consists of the following Python files:
Data_Fetcher_StockMarket_API.py: Fetches stock market data from the API and publishes it to Kafka topics.
Data_Comsumer_Stock_Market.py: Consumes data from Kafka topics and stores it in DynamoDB tables.
SNS_Service.py: Fetches data from DynamoDB tables and publishes it to SNS topics.

**Setting up Apache Kafka with Conduktor**
Conduktor Desktop is a desktop application that provides an easy way to run and manage Apache Kafka and other streaming data platforms on your local machine. Here's how you can set up a Kafka cluster using Conduktor:
Download and install Conduktor Desktop from the official website: https://www.conduktor.io/download/
Open Conduktor Desktop and create a new Kafka cluster by clicking on the "Add Cluster" button.
Choose the desired version of Apache Kafka and configure any additional settings if needed.
Start the Kafka cluster by clicking on the "Start" button.
Once the Kafka cluster is up and running, you can use the default Kafka broker address (127.0.0.1:9092) in your Python scripts to connect to the Kafka cluster locally.

**Usage**
Start the Apache Kafka cluster using Conduktor Desktop or your preferred method.
Run the Data_Fetcher_StockMarket_API.py script to fetch stock market data from the API and publish it to Kafka topics.
Run the Data_Comsumer_Stock_Market.py script to consume data from Kafka topics and store it in DynamoDB tables.
Run the SNS_Service.py script (or deploy it as an AWS Lambda function) to fetch data from DynamoDB tables and publish it to SNS topics.

**Note**
Please note that the code provided in this repository uses hardcoded values for various configurations, such as Kafka broker address, AWS credentials, DynamoDB table names, and SNS topic ARNs. You'll need to replace these values with your own configurations before running the project.
