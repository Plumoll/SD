import re, json, pika
from cos_backend import COSBackend
from collections import Counter

def main(args):
	s1 = json.dumps(args)
	args = json.loads(s1)
	odb = COSBackend(args["endpoint"], args['secret_key'], args['access_key'])
	counts = Counter()
	
	url = args["url"]
	params = pika.URLParameters(url)
	connection = pika.BlockingConnection(params)
	channel = connection.channel()
	channel.queue_declare(queue='map')

	fileFromServer = odb.get_object(args["bucket"], args["fileName"], extra_get_args={'Range':args["rang"]}).decode('UTF-8', errors='ignore')
	#stringFiltered = re.sub('[^A-Za-z0-9 \n]+', '', fileFromServer)
	#Delete unwanted characters
	stringFiltered = re.sub('[^A-Za-z \n]+', '', fileFromServer)
	#Split the string
	stringSplitted = re.split("\ |\n", stringFiltered)
	#Delete "" in array
	stringSplitted = list(filter(None, stringSplitted))

	counts.update(word.strip('.,?!"\'').lower() for word in stringSplitted)
	diccionary = dict(counts)
	
	dumped_json_string = json.dumps(diccionary)
	odb.put_object("magisd", args["fileName"]+args["functionNumber"], dumped_json_string)
	channel.basic_publish(exchange='', routing_key='WordCount', body=args["fileName"]+args["functionNumber"])
	connection.close()
	return {}
