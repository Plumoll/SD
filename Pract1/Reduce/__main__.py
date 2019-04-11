import pika, json, re
#! collections.abc?!?!?
from collections import Counter
from cos_backend import COSBackend

#dictionary used to save the result between callbacks
df = {}
#number of iterations done
n = 0
#number max of iterations
iterations = 0
#obd needed for the use of ibm COS
odb = None
#the name of the book we are analysing, needed for the result file's name
fileName = ""

#CountingWords function send its result as a body with this format: {'words': wordsNumber}
def CountingWordsCallback(ch, method, properties, body):
    global n
    global df
    global iterations

    #the body given is converted into a json
    received_data = json.loads(body.decode('UTF-8'))
    #create auxiliar dictionary
    daux = {}
    #daux = df + received data
    daux = Counter(df) + Counter(received_data)
    #counter -> dict
    df = dict(daux)
    n += 1
    #!for me
    print(n)
    print(df)

    if(n == iterations):
        #? can we put this into a function?
        global odb
        global fileName
        #dict -> json
        dumped_json_string = json.dumps(df)
        #save result to cloud  name:book.txt -> bookCountingWordResult.txt
        odb.put_object("magisd", fileName[:-4] + 'CountingWordResult.txt', dumped_json_string)
        #once we've saved the result the connection will be closed
        ch.stop_consuming()

#WordCount function saves the result in the COS and sends the file's name as body
def WordCountCallback(ch, method, properties, body):
        global df
        global n
        global odb
        global iterations

        #fileFromServer -> wordCount result. Got it from download the file its name is the body given
        fileFromServer = odb.get_object("magisd", body.decode('UTF-8')).decode('UTF-8', errors='ignore')
        #Delete the temporal file created by wordCount function
        odb.delete_object("magisd", body.decode('UTF-8', errors='ignore'))
        #String -> json
        received_data = json.loads(fileFromServer)

        #create auxiliar dictionary
        daux = {}
        #daux = df + received_data
        daux = Counter(df) + Counter(received_data)
        #counter -> dict
        df = dict(daux)
        n += 1
        #!for me
        print(n)
        if(n == iterations):
            #? can we put this into a function?
            global fileName
            #dictionary -> json
            dumped_json_string = json.dumps(df)
            #save result to cloud  name:book.txt -> bookWordCountResult.txt
            odb.put_object("magisd", fileName[:-4] + 'WordCountResult.txt', dumped_json_string)
            #once we've saved the result the connection will be closed
            ch.stop_consuming()
            
#def main(args):
def main(args):
    global odb
    global iterations
    global fileName
    
    #load necessary arguments
    s1 = json.dumps(args)
    args = json.loads(s1)
    odb = COSBackend(args["endpoint"], args["secret_key"], args["access_key"])
    url = args["url"]
    iterations = args["iter"]
    fileName = args["fileName"]

    #Pika configuration
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    #a queue for each function
    channel.queue_declare(queue="WordCount")
    channel.queue_declare(queue="CountingWords")
    #each queue has its own callback
    channel.basic_consume(WordCountCallback, queue="WordCount", no_ack=True)
    channel.basic_consume(CountingWordsCallback, queue="CountingWords", no_ack=True)
    #start receiving messages
    channel.start_consuming()
    #close rabbitmq's connection
    connection.close()
    return {}

