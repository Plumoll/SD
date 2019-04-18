import pika, json, re, yaml
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
    print(received_data)
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
def main():
    global odb
    global iterations
    global fileName
    global bucket
    
    #load necessary arguments
    with open('ibm_cloud_config', 'r') as config_file:
        res = yaml.safe_load(config_file)

    odb = COSBackend(res["ibm_cos"])
    url = res["rabbitmq"]["url"]
    iterations = 10
    fileName = 'prova'
    bucket = res["ibm_cos"]["bucket"]

     #Pika configuration
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    
    #a queue for each function
    result = channel.queue_declare('CountingWords')
    CountingWords_queue = result.method.queue
    result = channel.queue_declare('WordCount')
    WordCount_queue = result.method.queue

    
    #each queue has its own callback
    channel.basic_consume(WordCountCallback, queue=WordCount_queue, no_ack=True)
    channel.basic_consume(CountingWordsCallback, queue=CountingWords_queue, no_ack=True)
    #start receiving messages
    channel.start_consuming()
    #channel.stop_consuming()
    channel.queue_delete(queue=WordCount_queue)
    channel.queue_delete(queue=CountingWords_queue)
    #close rabbitmq's connection
    connection.close()

    return{}

if __name__ == '__main__':
    main()

