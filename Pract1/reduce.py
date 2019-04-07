import pika, json, yaml
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
    global iterations

    words += int(body)
    n += 1
    if(n == iterations):
        ch.stop_consuming()

def WordCountCallback(ch, method, properties, body):
        global df
        global n
        global odb
        global iterations

        fileFromServer = odb.get_object("magisd", body.decode('UTF-8')).decode('UTF-8')
        odb.delete_object("magisd", body.decode('UTF-8', errors='ignore'))
        received_data = json.loads(fileFromServer)

        daux = {}
        daux = Counter(df) + Counter(received_data)
        df = dict(daux)
        n += 1
        if(n == iterations):
            dumped_json_string = json.dumps(df)
            odb.put_object("magisd", "testfinal", dumped_json_string)
            ch.stop_consuming()
            
#def main(args):
def main():
    global odb
    global iterations
    global words

    with open('ibm_cloud_config', 'r') as config_file:
        res = yaml.safe_load(config_file)
    url = res["rabbitmq"]["url"]
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='WordCount')
    channel.queue_declare(queue='CountingWords')
    channel.basic_consume('WordCount', WordCountCallback, True)
    channel.basic_consume('CountingWords', CountingWordsCallback, True)
    
    odb = COSBackend(res['ibm_cos'])
    iterations=19

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
    connection.close()
    print(words)
    return words

if __name__ == '__main__':
    main()