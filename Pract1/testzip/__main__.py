import re, json, pika, yaml
from cos_backend import COSBackend
from collections import Counter

with open('ibm_cloud_config', 'r') as config_file:
	res = yaml.safe_load(config_file)
	#res['ibm_cos']['endpoint']
odb = COSBackend(res['ibm_cos'])
counts = Counter()
url = res["rabbitmq"]["url"]
print(url)
print('a')
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='map')

fileFromServer = odb.get_object('magisd', 'bible.txt', extra_get_args={'Range':'bytes=0-100'}).decode('UTF-8')
#stringFiltered = re.sub('[^A-Za-z0-9 \n]+', '', fileFromServer)
stringFiltered = re.sub('[^A-Za-z \n]+', '', fileFromServer)
stringSplitted = re.split("\ |\n", stringFiltered)
stringSplitted.remove("")

counts.update(word.strip('.,?!"\'').lower() for word in stringSplitted)
dumped_json_string = json.dumps(dict(counts))
channel.basic_publish(exchange='', routing_key='map', body=dumped_json_string)
connection.close()
#return (dict(counts))
