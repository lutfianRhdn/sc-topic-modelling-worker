from multiprocessing.connection import Connection
from .log import log
from typing import Any, Literal
import json
def sendMessage(
  conn:Connection,
  messageId:str,
  status:Literal["completed", "failed", "healthy", "unhealthy"],
  reason:str = "",
  destination:list[str] = ["supervisor"],
  data:Any = []
  ):
    message = {
        "messageId": messageId,
        "status": status,
        "reason": reason,
        "destination": destination,
        "data": data
    }
    conn.send(json.dumps(message))
    
def convertMessage(message)->dict:
    try:
        if isinstance(message, str):
            return json.loads(message)
        elif isinstance(message, dict):
            return message
        else:
            log(f"Unsupported message type: {type(message)}", "error")
            return {}
    except json.JSONDecodeError as e:
        log(f"Failed to decode message: {e}", "error")
        return {}