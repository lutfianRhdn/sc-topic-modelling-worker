import json
from multiprocessing.connection import Connection
import re
import threading
import uuid
import time

from openai import AzureOpenAI
from  utils.log import log 
from utils.handleMessage import sendMessage, convertMessage

from .Worker import Worker

class LLMWorker(Worker):
    ###############
    # dont edit this part
    ###############
    route_base = "/"
    conn:Connection
    requests: dict = {}
    def __init__(self):
        # we'll assign these in run()

        self.requests: dict = {}
        
    def run(self, conn: Connection, config:dict):
        # assign here
        LLMWorker.conn = conn

        #### add your worker initialization code here
        self.client = AzureOpenAI(
            api_version=config['azure']['api_version'],
            azure_endpoint=config['azure']['endpoint'],
            api_key=config['azure']['api_key'],
        )
        self.model_name=config['azure']['model']['completion']
        log("LLMWorker successfuly Running", "success")
        
        
        
        
        #### until this part
        # start background threads *before* blocking server
        threading.Thread(target=self.listen_task, daemon=True).start()
        threading.Thread(target=self.health_check, daemon=True).start()

        # asyncio.run(self.listen_task())
        self.health_check()


    def health_check(self):
        """Send a heartbeat every 10s."""
        while True:
            sendMessage(
                conn=LLMWorker.conn,
                messageId="heartbeat",
                status="healthy"
            )
            time.sleep(10)
    def listen_task(self):
        while True:
            try:
                if LLMWorker.conn.poll(1):  # Check for messages with 1 second timeout
                    message = self.conn.recv()
                    dest = [
                        d
                        for d in message["destination"]
                        if d.split("/", 1)[0] == "LLMWorker"
                    ]
                    destSplited = dest[0].split('/')
                    method = destSplited[1]
                    param= destSplited[2]
                    instance_method = getattr(self,method)
                    instance_method(id=param,data=message['data'],message=message)
            except EOFError:
                break
            except Exception as e:
              print(e)
              log(f"Listener error: {e}",'error' )
              break

    def sendToOtherWorker(self, destination, messageId: str, data: dict = None) -> None:
      sendMessage(
          conn=LLMWorker.conn,
          destination=destination,
          messageId=messageId,
          status="completed",
          reason="Message sent to other worker successfully.",
          data=data or {}
      )
    ##########################################
    # add your worker methods here
    ##########################################
    
    def getContext(self,id,data,message):
        topics= data['topics']
        keyword = data['keyword']
        best_num_topics_str = data['num_of_topic']
        start_date = data['start_date']
        end_date = data['end_date']
        log(f"LLMWorker getContext called with id: {id}, topics: {topics}, keyword: {keyword}, best_num_topics_str: {best_num_topics_str}", "info")
        # print(topics)
        completion = self.client.chat.completions.create(
            model=self.model_name,
            messages=[
                {
                    "role": "system",
                    "content": "Anda adalah AI Linguistik yang dapat menentukan kalimat dari beberapa topik hasil dari proses topic modeling yang berupa kumpulan kata-kata,  dengan mempertimbangkan bobot setiap topik yang ada, dalam merangkai kata-kata kunci menjadi kalimat yang padu untuk sebuah topik yang diperbincangkan di Twitter dengan mengambil kata dari hasil topic modeling lalu menyusunnya menjadi sebuah kalimat yang padu yang mudah dipahami."
                },
                {
                    "role": "user",
                    "content": f"Topik ini membahas tentang keyword: {keyword} dengan berbagai pandangan masyarakat terhadap topik tersebut dengan hasil topic modeling dengan {best_num_topics_str} topik terdiri dari beberapa kata kunci berikut: {topics} Buatkan dengan format JSON dengan 1 topik untuk 1 kalimat utama dengan jumlah sesuai jumlah topik yang diberikan yaitu: {best_num_topics_str}. Berikut ini adalah format JSON-nya: \n            [\n                {{\n                    \"kata_kunci\": \"...\"\n                    \"kalimat\": Topik ini tentang \"...\"\n                }}\n                ...\n            ]\n            ONLY answer in JSON FORMAT without opening words. "
                }
            ],
        )
        generated_sentence = completion.choices[0].message.content or ""
        pattern = re.compile(r'\[(?:\s*{[^{}]*}\s*,?)*\s*\]')
        match = pattern.search(generated_sentence)

        if match:
            json_text = match.group()
            # print(json_text)
        else:
            print("Tidak ada JSON yang ditemukan dalam string.")
        # Print the generated sentence
        res_json = json.loads(json_text)
        res =[]
        for index,item in enumerate(res_json):
            res.append({
                "topicId":index,
                "projectId":id,
                "context":item['kalimat'],
                "words":[word.lstrip() for word in  item['kata_kunci'].split(',')]
            })
        self.sendToOtherWorker(
            destination=[f"DatabaseInteractionWorker/saveContext/{id}"],
            messageId=message['messageId'],
            data={
                "contexts":res,
                "keyword": keyword,
                "start_date": start_date,
                "end_date": end_date,
                }
        )
# topicId
# 0
# projectId
# "678e08cd7ae271700e0f74ef"
# context
# "An error occurred: Error communicating with OpenAI: HTTPSConnectionPoo…"

# words
# Array (10)
# keyword
# "makan gratis"

        # for index, item in enumerate(res_json):
        #     res['context'] += str(index+1)+". "+item['kalimat']+"<br/>"
        #     res['interpretation'].append({
        #         "word_topic": item['kata_kunci'],
        #         "word_interpretation": item['kalimat']
        #     })
            
        
        # print(res)
        # return res 

def main(conn: Connection, config: dict):
    worker = LLMWorker()
    worker.run(conn, config)
