from multiprocessing.connection import Connection
from flask import Flask, request, jsonify
from flask_classful import FlaskView, route
import threading
import uuid
import asyncio
import time
import utils.log as log
from utils.handleMessage import sendMessage, convertMessage

app = Flask(__name__)

class RestApiWorker(FlaskView):
    ###############
    # dont edit this part
    ###############
    route_base = "/"
    conn:Connection
    requests: dict = {}
   
        
    def run(self, conn: Connection, port: int):
        # assign here
        RestApiWorker.conn = conn
        self._port = port

        RestApiWorker.register(app)

        # start background threads *before* blocking server

        app.run(debug=True, port=self._port, use_reloader=False,host="0.0.0.0")
        asyncio.run(self.listen_task())


    async def listen_task(self):
        while True:
            try:
                if RestApiWorker.conn.poll(1):  # Check for messages with 1 second timeout
                    raw = RestApiWorker.conn.recv()
                    msg = convertMessage(raw)
                    self.onProcessed(raw)
            except EOFError:
                break
            except Exception as e:
                log(f"Listener error: {e}",'error' )
                break

    def onProcessed(self, msg: dict):
        """
        Called when a worker response comes in.
        msg must contain 'messageId' and 'data'.
        """
        task_id = msg.get("messageId")
        entry = RestApiWorker.requests[task_id]
        if not entry:
            return
        entry["response"] = msg.get("data")
        entry["event"].set()
    def sendToOtherWorker(self, destination: str, data):
      task_id = str(uuid.uuid4())
      evt = threading.Event()
      
      RestApiWorker.requests[task_id] = {
          "event": evt,
          "response": None
      }
      print(f"Sending request to {destination} with task_id: {task_id}")
      
      sendMessage(
          conn=RestApiWorker.conn,
          messageId=task_id,
          status="processing",
          destination=destination,
          data=data
      )
      if not evt.wait(timeout=10):
          # timeout
          return {
              "taskId": task_id,
              "status": "timeout",
              "result": None
          }
      
      # success
      result = RestApiWorker.requests.pop(task_id)["response"]
      return {
          "taskId": task_id,
          "status": "completed",
          "result": result
      }

    ##########################################
    # FLASK ROUTES FUNCTIONS
    ##########################################
    @route('/', methods=['GET'])
    def getData(self):

        return jsonify({
            "message": "Welcome to the Rest API Worker",
            "status": "success"
        }), 200
    @route('/topic-by-project/<projectId>', methods=['GET'])
    def getTopicByProjectId(self, projectId) :
        """
        Get topic by project id
        """
        result = self.sendToOtherWorker(
            destination=[f"CacheWorker/getByKey/topic_{project_id}"],
            data={"project_id": "topic_{projectId}",}
        )
        if len(result["result"]) == 0:
            destination = [f"DatabaseInteractionWorker/getTopicByProjectId/{projectId}"]
            result = self.sendToOtherWorker(destination, data)
             sendMessage(
                conn=RestApiWorker.conn,
                messageId=str(uuid.uuid4()),
                status="processing",
                destination=['CacheWorker/set/topic_' + projectId ],
                data={
                    "key":f"topic_{projectId}",
                    "value":result,
                }
            )
        return jsonify(result), 200
    @route('/document-by-project/<projectId>', methods=['GET'])
    def getDocumentByProjectId(self, projectId):
        """
        Get documents by project id
        """
           result = self.sendToOtherWorker(
            destination=[f"CacheWorker/getByKey/doc_{project_id}"],
            data={"project_id": "topic_{projectId}",}
        )
        if len(result["result"]) == 0:
        destination =[ f"DatabaseInteractionWorker/getDocumentByProjectId/{projectId}"]
            result = self.sendToOtherWorker(destination, data)
             sendMessage(
                conn=RestApiWorker.conn,
                messageId=str(uuid.uuid4()),
                status="processing",
                destination=['CacheWorker/set/doc_' + projectId ],
                data={
                    "key":f"doc_{projectId}",
                    "value":result,
                }
            )
        return jsonify(result), 200
        result = self.sendToOtherWorker(destination, data)
        return jsonify(result), 200
    

def main(conn: Connection, config: dict):
    worker = RestApiWorker()
    worker.run(conn, config.get("port", 5000))
