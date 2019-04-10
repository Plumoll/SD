import pika, json, re
from collections import Counter
from cos_backend import COSBackend

df = {}
n = 0
words = 0
iterations = 0
odb = None

def CountingWordsCallback(ch, method, properties, body):
    global words
    global n
    global odb
    global iterations

    words += int(body)
    n += 1
    print(n)
    if(n == iterations):
        stringFiltered = re.sub('[^A-Za-z \n]+', '', body.decode('UTF-8'))
        dumped_json_string = json.dumps({"words": words})
        odb.put_object("magisd", stringFiltered[:-3]+'ResultCountingWords.txt', dumped_json_string)
        ch.stop_consuming()

def WordCountCallback(ch, method, properties, body):
        global df
        global n
        global odb
        global iterations

        fileFromServer = odb.get_object("magisd", body.decode('UTF-8')).decode('UTF-8', errors='ignore')
        odb.delete_object("magisd", body.decode('UTF-8', errors='ignore'))
        received_data = json.loads(fileFromServer)

        daux = {}
        daux = Counter(df) + Counter(received_data)
        df = dict(daux)
        n += 1
        print(n)
        if(n == iterations):
            stringFiltered = re.sub('[^A-Za-z \n]+', '', body.decode('UTF-8'))
            dumped_json_string = json.dumps(df)
            odb.put_object("magisd", stringFiltered[:-3]+'ResultWordCount.txt', dumped_json_string)
            ch.stop_consuming()
            
#def main(args):
def main(args):
    global odb
    global iterations
    global words
    
    s1 = json.dumps(args)
    args = json.loads(s1)
    odb = COSBackend(args["endpoint"], args['secret_key'], args['access_key'])
    url = args["url"]
    iterations=args["iter"]

    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='WordCount')
    channel.queue_declare(queue='CountingWords')
    channel.basic_consume(WordCountCallback, queue="WordCount", no_ack=True)
    channel.basic_consume(CountingWordsCallback, queue='CountingWords', no_ack=True)
    
    

    channel.start_consuming()
    connection.close()
    return {}

