from .env import database,port,azure,rabbitmq

RestApiWorkerConfig = {
    "port": port
}
DatabaseInteractionWorkerConfig = {
  "connection_string": database["connection_string"],
  "database": database["database"],
  "tweet_database": database["tweet_database"]
}
RabbitMQWorkerConfig = {
    "connection_string": rabbitmq["url"],
    "consumeQueue": rabbitmq["consume"]["queue"],
    "consumeCompensationQueue": rabbitmq["consume"]["compensation_queue"],
    "produceQueue": rabbitmq["produce"]["queue"],
    "produceCompensationQueue": rabbitmq["produce"]["compensation_queue"]
}

PreprocessingWorkerConfig = {
  "azure": {
    "endpoint": azure["endpoint"],
    "api_key": azure["api_key"],
    "api_version": azure['api_version'],
    "model": {
        "completion": azure["model"]["completion"]
    }
  },
}

ETMWorkerConfig = {
}
  
LLMWorkerConfig = {
  "azure":{
    "endpoint": azure["endpoint"],
    "api_key": azure["api_key"],
    "api_version": azure['api_version'],
    "model": {
        # "embedding": azure["model"]["embedding"],
        "completion": azure["model"]["completion"]
    }
  }
}

CacheWorkerConfig={
    "redis_url": redis['host'], # type: ignore
    "redis_port": redis['port'], # type: ignore
    "redis_db": redis['db'], # type: ignore
    "redis_username": redis['username'], # type: ignore
    "redis_password": redis['password'], # type: ignore

} # type: ignore

allConfigs ={
    "DatabaseInteractionWorker": DatabaseInteractionWorkerConfig,
    "RestApiWorker": RestApiWorkerConfig,
    "PreprocessingWorker": PreprocessingWorkerConfig,
    "ETMWorker": ETMWorkerConfig,
    "LLMWorker": LLMWorkerConfig,
    "RabbitMQWorker": RabbitMQWorkerConfig,
    "CacheWorkerConfig": CacheWorkerConfig
}