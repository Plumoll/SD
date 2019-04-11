import re, json, pika
from cos_backend import COSBackend

def main(args):
	s1 = json.dumps(args)
	args = json.loads(s1)
	odb = COSBackend(args["endpoint"], args['secret_key'], args['access_key'])
	
	url = args["url"]
	params = pika.URLParameters(url)
	connection = pika.BlockingConnection(params)
	channel = connection.channel()
	channel.queue_declare(queue='CountingWords')

	fileFromServer = odb.get_object(args["bucket"], args["fileName"], extra_get_args={'Range':args["rang"]}).decode('UTF-8', errors='ignore')
	#stringFiltered = re.sub('[^A-Za-z0-9 \n]+', '', fileFromServer)
	#Delete unwanted characters
	stringFiltered = re.sub('[^A-Za-z \n]+', '', fileFromServer)
	#Split the string
	stringSplitted = re.split("\ |\n", stringFiltered)
	#Delete "" in array
	stringSplitted = list(filter(None, stringSplitted))

	body = json.dumps({"words":len(stringSplitted)})
	channel.basic_publish(exchange='', routing_key='CountingWords', body=body)
	connection.close()
	return {}
