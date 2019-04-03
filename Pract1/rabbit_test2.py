import pika, json, yaml


with open('ibm_cloud_config', 'r') as config_file:
    res = yaml.safe_load(config_file)
url = res["rabbitmq"]["url"]
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='map')
sent_data = {'a': 2, 'b': 3}
dumped_json_string = json.dumps(sent_data)
channel.basic_publish(exchange='', routing_key='map', body=dumped_json_string)
connection.close()