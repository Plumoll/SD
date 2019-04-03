import pika, json, yaml
from collections import Counter

df = {}
n = 0

def callback(ch, method, properties, body):
        received_data = json.loads(body)
        global df
        global n
        daux = {}
        daux = Counter(df) + Counter(received_data)
        df = dict(daux)
        n = n + 1
        if(n==10):
            ch.stop_consuming()
        #print(" [x] Received %r" % received_data)
        #print("\n\n\n\n\n{0}".format(n))

#def main(args):
def main():
    with open('ibm_cloud_config', 'r') as config_file:
        res = yaml.safe_load(config_file)
    url = res["rabbitmq"]["url"]
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='map')
    channel.basic_consume('map', callback, True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
    connection.close()
    #return daux
    global df
    print(dict(df))

if __name__ == '__main__':
    main()