import re, json, pika
from cos_backend import COSBackend
from collections import Counter

def main(args):
	s1 = json.dumps(args)
	args = json.loads(s1)
	print(args)
	odb = COSBackend(args["endpoint"], args['secret_key'], args['access_key'])
	counts = Counter()
	
	url = args["url"]
	params = pika.URLParameters(url)
	connection = pika.BlockingConnection(params)
	channel = connection.channel()
	channel.queue_declare(queue='map')

	fileFromServer = odb.get_object(args["bucket"], args["fileName"], extra_get_args={'Range':args["rang"]}).decode('UTF-8')
	#stringFiltered = re.sub('[^A-Za-z0-9 \n]+', '', fileFromServer)
	stringFiltered = re.sub('[^A-Za-z \n]+', '', fileFromServer)
	stringSplitted = re.split("\ |\n", stringFiltered)
	# while("" in stringSplitted):
	# 	stringSplitted.remove("")

	counts.update(word.strip('.,?!"\'').lower() for word in stringSplitted)
	aux = dict(counts).pop("")
	dumped_json_string = json.dumps(dict(counts))
	channel.basic_publish(exchange='', routing_key='map', body=dumped_json_string)
	connection.close()
	return {}
	#return (dict(counts))
