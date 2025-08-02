from datetime import datetime
from multiprocessing.connection import Connection
import traceback

from pymongo import MongoClient
import asyncio
from utils.log import log
from utils.handleMessage import sendMessage
import time


from .Worker  import Worker
class DatabaseInteractionWorker(Worker):
  #################
  # dont edit this part
  ################
  _isBusy: bool = False
  _client: MongoClient 
  _db: str 
  _dbTweet:str
  def __init__(self, conn: Connection, config: dict):
    self.conn=conn
    self._db = config.get("database", "mydatabase") 
    self._dbTweet = config.get("tweet_database", "tweets")
    self.connection_string = config.get("connection_string", "mongodb://localhost:27017/") 

  def run(self) -> None:
    self._client = MongoClient(self.connection_string)
    self._db= self._client[self._db]
    self._dbTweet = self._client[self._dbTweet]
    if not self._client:
      log("Failed to connect to MongoDB", "error")
    log(f"Connected to MongoDB at {self.connection_string}", "success")
    asyncio.run(self.listen_task())

  
  async def listen_task(self) -> None:
    while True:
      try:
          if self.conn.poll(1):  # Check for messages with 1 second timeout
              message = self.conn.recv()
              dest = [
                  d
                  for d in message["destination"]
                  if d.split("/", 1)[0] == "DatabaseInteractionWorker"
              ]
              # dest = [d for d in message['destination'] if d ='DatabaseInteractionWorker']
              destSplited = dest[0].split('/')
              method = destSplited[1]
              param= destSplited[2]
              instance_method = getattr(self,method)
              result = instance_method(id=param,data=message.get("data", None))
              sendMessage(
                  conn=self.conn, 
                  status="completed",
                  destination=result["destination"],
                  messageId=message["messageId"],
                  data=convertObjectIdToStr(result.get('data', [])),
              )
      except EOFError:
          log("Connection closed by supervisor",'error')
          break
      except Exception as e:
          log(f"Message loop error: {e}",'error')
          break
  
  #########################################
  # Methods for Database Interaction
  #########################################
  

  def saveContext(self, id, data):
    contexts = data['contexts']
    keyword= data['keyword']
    start_date = data['start_date']
    end_date = data['end_date']
    alreadyExists = self._db['topics'].find({"projectId": id})
    if len(list(alreadyExists)) == 0:
      log(f"Project with id {id} already exists in topics collection.", "error")
      self._db['topics'].insert_many(contexts)
    print({
          "topics": [context['context'] for context in contexts ],
          "keyword": keyword,
          "start_date": start_date,
          "end_date": end_date
        })
    return {
      "data": 
        {
          "topics": [context['context'] for context in contexts ],
          "keyword": keyword,
          "projectId":id,
          "start_date": start_date,
          "end_date": end_date
        },
      "destination": ["RabbitMQWorker/produceMessage/"]}
    pass
  def getTopicByProjectId(self,id, data):
    topicProject =  self._db['topics'].find_one(
        {"projectId": id},
        {"_id": 0}
    )
    return {"data": topicProject, "destination": ["RestApiWorker/onProcessed"]}
  def getDocumentsByProjectId(self, id, data):
    documentsProject = self._db['documents'].find(
        {"projectId": id},
        {"_id": 0}
    )
    documentsList = list(documentsProject)
    if not documentsList:
      log(f"No documents found for project {id}.", "info")
      return {"data": [], "destination": ["RestApiWorker/onProcessed"]}
    
    return {
        "data": documentsList,
        "destination": ["RestApiWorker/onProcessed"]
    }
  def getTweetByKeyword(self,id,data):
    keyword = data['keyword']
    start_date = data['start_date']
    end_date = data['end_date']
    match_stage = {
            '$match': {
                'full_text': {'$regex': keyword.replace(' ','|'), '$options': 'i'}
            }
        }
        
    pipeline = [match_stage]

    # Add date filtering if both start_date and end_date are provided
    if start_date and end_date:
        start_datetime = datetime.strptime(f"{start_date} 00:00:00 +0000", "%Y-%m-%d %H:%M:%S %z")
        end_datetime = datetime.strptime(f"{end_date} 23:59:59 +0000", "%Y-%m-%d %H:%M:%S %z")
        print(f"Filtering tweets from {start_datetime} to {end_datetime}")
        
        add_fields_stage = {
            '$addFields': {
                'parsed_date': {'$toDate': '$created_at'}
            }
        }
        match_date_stage = {
            '$match': {
                'parsed_date': {
                    '$gte': start_datetime,
                    '$lte': end_datetime
                }
            }
        }

        pipeline.extend([add_fields_stage, match_date_stage])
    
    # Project stage to include only specific fields
    project_stage = {
        '$project': {
            '_id' : 0,
            'full_text': 1,
            'username': 1,
            'in_reply_to_screen_name': 1,
            'tweet_url': 1
        }
    }
    pipeline.append(project_stage)
    
    # Execute the aggregation pipeline
    cursor = self._dbTweet['tweets'].aggregate(pipeline)
    return {
        "data": {
          "tweets":list(cursor),
          "keyword": keyword,
          'start_date': start_date,
          'end_date': end_date
          },
        "destination": [f"PreprocessingWorker/run_preprocessing/{id}"]
    }
  def saveDocuments(self, id, data):
    documents = data['documents']
    keyword = data['keyword']
    start_date = data['start_date']
    end_date = data['end_date']
    if not documents:
      log(f"No documents to save for project {id}.", "error")
      return {"data": [], "destination": ["RestApiWorker/onProcessed"]}
    
    # Check if the project already exists
    alreadyExists = self._db['documents'].find({"projectId": id})
    if len(list(alreadyExists)) == 0:
      log(f"Project with id {id} already exists in documents collection.", "error")
      self._db['documents'].insert_many(documents)
    
    return {
        "data": {
          "documents": documents,
          "keyword": keyword,
          'projectId': id,
          'start_date': start_date,
          'end_date': end_date
        },
        "destination": ["RabbitMQWorker/produceMessage/"]
    }
  def deleteTopicByProjectId(self, id, data):
    result = self._db['topics'].delete_many({"projectId": id})
    if result.deleted_count > 0:
        log(f"Deleted {result.deleted_count} topics for project {id}.", "success")
    else:
        log(f"No topics found for project {id}.", "info")
    return {"data": {"deleted_count": result.deleted_count}, "destination": ["RestApiWorker/onProcessed"]}
  def deleteDocumentsByProjectId(self, id, data):
    result = self._db['documents'].delete_many({"projectId": id})
    if result.deleted_count > 0:
        log(f"Deleted {result.deleted_count} documents for project {id}.", "success")
    else:
        log(f"No documents found for project {id}.", "info")
    return {"data": {"deleted_count": result.deleted_count}, "destination": ["RestApiWorker/onProcessed"]}
  
    
  # def getTextTweetsByKeyword(self, id, data):
     
############### Helper function to convert ObjectId to string in a list of documents
  
def convertObjectIdToStr(data: list) -> list:
  import traceback
  try:
      res = []
      print(f"Converting ObjectId to string for {len(data)} documents")
      if type(data) is not list:
        print("Data is not a list, returning it as is.")
        return data 
      for doc in data:
          # print(f"Converting document: {doc}")

          if isinstance(doc, dict):
              doc["_id"] = str(doc["_id"]) if "_id" in doc else ""
              res.append(doc)
          else:
              print(f"Skipping non-dict item: {doc}")
      return res
  except Exception as e:
      traceback.print_exc()
      print(f"Error converting ObjectId to string: {e}")
      return []

# This is the main function that the supervisor calls


def main(conn: Connection, config: dict):
    """Main entry point for the worker process"""
    worker = DatabaseInteractionWorker(conn, config)
    worker.run()