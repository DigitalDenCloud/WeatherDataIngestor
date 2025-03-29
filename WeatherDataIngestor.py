import json
import os
import urllib3
import boto3
import random
from datetime import datetime
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
firehose_client = boto3.client('firehose')
secretsmanager_client = boto3.client('secretsmanager')
http = urllib3.PoolManager()

# Constants
DELIVERY_STREAM_NAME = os.environ.get('DELIVERY_STREAM_NAME', 'weather-data-stream')
SECRET_NAME = os.environ.get('WEATHER_API_SECRET_NAME')
DEFAULT_CITY = os.environ.get('DEFAULT_CITY', 'London')
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"

# Optional list of cities to randomly select from
RANDOM_CITIES = ['London', 'New York', 'Paris', 'Berlin', 'Tokyo', 'Sydney', 'Cairo', 'Toronto', 'Istanbul']

def get_api_key():
    """Retrieve API key from Secrets Manager"""
    try:
        if not SECRET_NAME:
            logger.warning("No secret name provided, using environment variable")
            return os.environ.get('OpenWeatherMapApiKey')
            
        logger.info(f"Retrieving API key from secret: {SECRET_NAME}")
        response = secretsmanager_client.get_secret_value(SecretId=SECRET_NAME)
        secret = response['SecretString']
        
        # The secret could be a JSON string or just the key itself
        try:
            secret_dict = json.loads(secret)
            return secret_dict.get('OpenWeatherMapApiKey')
        except json.JSONDecodeError:
            return secret
            
    except Exception as e:
        logger.error(f"Error retrieving API key from Secrets Manager: {str(e)}")
        # Fall back to environment variable
        return os.environ.get('OpenWeatherMapApiKey')

def kelvin_to_celsius(kelvin):
    """Convert temperature from Kelvin to Celsius"""
    return round(kelvin - 273.15, 2)

def transform_weather_data(data):
    """Transform weather data into desired format, including partition fields."""
    current_time = datetime.utcnow()
    # Use Unix epoch time for event_timestamp so that Firehose JQ's strftime can process it
    event_timestamp = int(current_time.timestamp())
    
    transformed_data = {
        'city': data['name'],
        'country': data['sys']['country'],
        'weather_condition': data['weather'][0]['main'],
        'weather_description': data['weather'][0]['description'],
        'event_timestamp': event_timestamp  # Unix epoch integer
    }
    
    # Temperature conversions
    if 'main' in data:
        transformed_data.update({
            'temperature_celsius': kelvin_to_celsius(data['main']['temp']),
            'feels_like_celsius': kelvin_to_celsius(data['main']['feels_like']),
            'temp_min_celsius': kelvin_to_celsius(data['main']['temp_min']),
            'temp_max_celsius': kelvin_to_celsius(data['main']['temp_max']),
            'humidity_percent': data['main']['humidity'],
            'pressure_hpa': data['main']['pressure']
        })
    
    # Wind data
    if 'wind' in data:
        transformed_data.update({
            'wind_speed_ms': data['wind'].get('speed'),
            'wind_direction_degrees': data['wind'].get('deg')
        })
    
    # Sunrise/sunset times
    if 'sys' in data and 'sunrise' in data['sys'] and 'sunset' in data['sys']:
        transformed_data.update({
            'sunrise': datetime.utcfromtimestamp(data['sys']['sunrise']).strftime('%Y-%m-%d %H:%M:%S UTC'),
            'sunset': datetime.utcfromtimestamp(data['sys']['sunset']).strftime('%Y-%m-%d %H:%M:%S UTC')
        })
    
    return transformed_data

def lambda_handler(event, context):
    try:
        # Get API key
        api_key = get_api_key()

        # Determine city to fetch
        city = event.get('city') or DEFAULT_CITY or random.choice(RANDOM_CITIES)
        logger.info(f"Fetching weather data for: {city} (source: event input, environment variable, or random fallback)")

        if not api_key:
            raise ValueError("No API key available. Set WEATHER_API_KEY environment variable or configure secret.")
        
        # Fetch weather data
        url = f"{WEATHER_API_URL}?q={city}&appid={api_key}"
        response = http.request('GET', url)
        
        if response.status != 200:
            error_msg = f"Weather API error: Status {response.status}, Response: {response.data.decode('utf-8')}"
            logger.error(error_msg)
            return {
                'statusCode': response.status,
                'body': json.dumps({'error': error_msg})
            }
        
        # Process the data
        weather_data = json.loads(response.data.decode('utf-8'))
        transformed_data = transform_weather_data(weather_data)
        
        # Convert to JSON string with newline for better Firehose processing
        json_data = json.dumps(transformed_data) + '\n'
        
        # Send to Firehose
        logger.info(f"Sending weather data to Firehose stream: {DELIVERY_STREAM_NAME}")
        response = firehose_client.put_record(
            DeliveryStreamName=DELIVERY_STREAM_NAME,
            Record={'Data': json_data.encode('utf-8')}
        )
        
        logger.info(f"Successfully sent data to Firehose. Response: {response}")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Weather data successfully sent to Firehose',
                'city': city,
                'recordId': response.get('RecordId')
            })
        }
        
    except Exception as e:
        error_message = f"Error processing weather data: {str(e)}"
        logger.error(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }