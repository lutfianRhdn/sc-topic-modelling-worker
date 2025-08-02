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

allConfigs ={
    "DatabaseInteractionWorker": DatabaseInteractionWorkerConfig,
    "RestApiWorker": RestApiWorkerConfig,
    "PreprocessingWorker": PreprocessingWorkerConfig,
    "ETMWorker": ETMWorkerConfig,
    "LLMWorker": LLMWorkerConfig,
    "RabbitMQWorker": RabbitMQWorkerConfig
}