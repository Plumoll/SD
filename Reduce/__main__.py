import pika, json, re
from collections import Counter
from cos_backend import COSBackend

#dictionary used to save the result between callbacks
df = {}
#number of iterations done
n = None
#number max of iterations
iterations = None
#obd needed for the use of ibm COS
odb = None
#the name of the book we are analysing, needed for the result file's name
fileName = ""
#COS's bucket where the files will be saved
bucket = ""

#CountingWords function send its result as a body with this format: {'words': wordsNumber}
def CountingWordsCallback(ch, method, properties, body):
    global n
    global df
    global iterations

    #the body given is converted into a json
    received_data = json.loads(body.decode('UTF-8'))

    #Combine fileFromServer with previous work
    daux = {}
    daux = Counter(df) + Counter(received_data)
    df = dict(daux)

    n += 1

    if(n == iterations):
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
        global bucket

        #fileFromServer -> wordCount result. Got it from download the file its name is the body given
        fileFromServer = odb.get_object(bucket, body.decode('UTF-8')).decode('UTF-8', errors='ignore')
        #Delete the temporal file created by wordCount function
        odb.delete_object(bucket, body.decode('UTF-8', errors='ignore'))
        #String -> json
        received_data = json.loads(fileFromServer)

        #Combine fileFromServer with previous work
        daux = {}
        daux = Counter(df) + Counter(received_data)
        df = dict(daux)

        n += 1
        if(n == iterations):
            global fileName
            #dictionary -> json
            dumped_json_string = json.dumps(df)
            #save result to cloud  name:book.txt -> bookWordCountResult.txt
            odb.put_object(bucket, fileName[:-4] + 'WordCountResult.txt', dumped_json_string)
            #once we've saved the result the connection will be closed
            ch.stop_consuming()
            
def main(args):
    #global variables are needed since callback methods have always the same parameters (ch, method, properties, body)
    global odb
    global iterations
    global fileName
    global bucket
    global n
    global df
    
    #load necessary arguments
    s1 = json.dumps(args)
    args = json.loads(s1)
    res = args["res"]

    #inicialize global variables
    odb = COSBackend(res["ibm_cos"])
    bucket = res["ibm_cos"]["bucket"]
    iterations = args["iter"]
    fileName = args["fileName"]
    n = 0
    df = {}

    #Pika configuration
    url = res["rabbitmq"]["url"]
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    
    #a different queue for each function
    channel.queue_declare('CountingWords')
    channel.queue_declare('WordCount')

    #each queue has its own callback
    channel.basic_consume(WordCountCallback, queue='CountingWords', no_ack=True)
    channel.basic_consume(CountingWordsCallback, queue='WordCount', no_ack=True)
    #start receiving messages
    channel.start_consuming()

    #close rabbitmq's connection
    connection.close()

    return{}

