import os
from dotenv import load_dotenv
load_dotenv()

port = int(os.getenv("PORT", 5000))

database= {
    "connection_string": os.getenv("DB_CONNECTION_STRING", "mongodb://localhost:27017/"),
    "database": os.getenv("DB_NAME", "mydatabase"),
    "tweet_database": os.getenv("DB_TWEETS", "tweets"),
}
azure={
    'endpoint': os.getenv("AZURE_OPENAI_ENDPOINT", "https://your-azure-endpoint.openai.azure.com/"),
    "api_key": os.getenv("AZURE_OPENAI_API_KEY", "your-azure-api-key"),
    "api_version": os.getenv("AZURE_OPENAI_API_VERSION", "2023-05-15"),
    "model":{
        # "embedding": os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME_EMBEDDING", "text-embedding-3-large"),
        "completion": os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4.1"),
    }
}

rabbitmq = {
    "url": os.getenv("RABBITMQ_URL", "amqp://localhost:5672/"),
    "consume":{
        "queue": os.getenv("RABBITMQ_CONSUME_QUEUE", "consume_queue"),
        "compensation_queue": os.getenv("RABBITMQ_COMPENSATION_QUEUE", "consume_compensation_queue"),
        "exchange": os.getenv("RABBITMQ_CONSUME_EXCHANGE", "consume_exchange")
    },
    "produce":{
        "queue": os.getenv("RABBITMQ_PRODUCE_QUEUE", "produce_queue"),
        "compensation_queue": os.getenv("RABBITMQ_PRODUCE_COMPENSATION_QUEUE", "produce_compensation_queue"),
        "exchange": os.getenv("RABBITMQ_EXCHANGE", "produce_exchange")
    }
}

redis={
    "host": os.getenv("REDIS_URL", "localhost"),
    "port": int(os.getenv("REDIS_PORT", 6379)),
    "db": int(os.getenv("REDIS_DB", 0)),
    "username":os.getenv("REDIS_USERNAME", ""),
    "password": os.getenv("REDIS_PASSWORD", "")
}