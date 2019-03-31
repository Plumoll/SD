import pika, json, yaml


with open('ibm_cloud_config', 'r') as config_file:
    res = yaml.safe_load(config_file)
url = res["rabbitmq"]["url"]
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='map')

def callback(ch, method, properties, body):
    received_data = json.loads(body)
    print(" [x] Received %r" % received_data)

channel.basic_consume('map', callback, True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
connection.close()