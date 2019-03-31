import pika, json, yaml
from collections import Counter

daux = {}
n = 0

def callback(ch, method, properties, body):
        received_data = json.loads(body)
        global daux
        global n
        df = Counter(daux) + Counter(body)
        daux = df
        n = n + 1
        if(n==2):
            ch.stop_consuming()
        print(" [x] Received %r" % received_data)

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
    print(daux)

if __name__ == '__main__':
    main()