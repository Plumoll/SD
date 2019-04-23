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
bucket = ""

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
        global bucket
        #dict -> json
        dumped_json_string = json.dumps(df)
        #save result to cloud  name:book.txt -> bookCountingWordResult.txt
        odb.put_object(bucket, fileName[:-4] + 'CountingWordResult.txt', dumped_json_string)
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
            global bucket
            #dictionary -> json
            dumped_json_string = json.dumps(df)
            #save result to cloud  name:book.txt -> bookWordCountResult.txt
            odb.put_object(bucket, fileName[:-4] + 'WordCountResult.txt', dumped_json_string)
            #once we've saved the result the connection will be closed
            ch.stop_consuming()
            
#def main(args):
def main(args):
    global odb
    global iterations
    global fileName
    global bucket
    
    #load necessary arguments
    s1 = json.dumps(args)
    args = json.loads(s1)
    res = args["res"]
	#configure COS library
    odb = COSBackend(res["ibm_cos"])
    bucket = res["ibm_cos"]["bucket"]
    url = res["rabbitmq"]["url"]
    iterations = args["iter"]
    fileName = args["fileName"]

    #Pika configuration
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    #a queue for each function
    channel.exchange_declare(exchange='direct_logs', exchange_type='direct')

    result = channel.queue_declare('CountingWords', True)
    queue_name_CountingWords = result.method.queue
    result = channel.queue_declare('WordCount', True)
    queue_name_wordCount = result.method.queue

    channel.queue_bind(exchange='direct_logs',queue=queue_name_CountingWords,routing_key="CountingWords")
    channel.queue_bind(exchange='direct_logs',queue=queue_name_wordCount,routing_key="WordCount")
    #each queue has its own callback
    channel.basic_consume(WordCountCallback, queue=queue_name_wordCount, no_ack=True)
    channel.basic_consume(CountingWordsCallback, queue=queue_name_CountingWords, no_ack=True)
    #start receiving messages
    channel.start_consuming()
    #channel.stop_consuming()
    channel.queue_delete(queue=queue_name_CountingWords)
    channel.queue_delete(queue=queue_name_wordCount)
    # channel.queue_unbind(exchange='direct_logs',queue=queue_name_CountingWords,routing_key="CountingWords")
    # channel.queue_unbind(exchange='direct_logs',queue=queue_name_wordCount,routing_key="WordCount")
    # channel.exchange_delete(exchange='direct_logs')
    # channel.close()
    #close rabbitmq's connection
    connection.close()

    return{}

