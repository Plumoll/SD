import re, json, pika
from cos_backend import COSBackend

#this function calcules a bit position which doesn't cut any word
#   comments:
#       it starts searching from the end and when it finds an space returns the value
#       the range of data downloaded for this is 20 since the average words lenth is more or less 5 so it will have an space always
#   param:
#       fileName    ->  name of the file
#       rang        ->  rang from which we want to analyse
#       res         ->  loaded configuration from ibm_cloud_config
def selectRange(fileName, rang, res):
	odb = COSBackend(res['ibm_cos'])
	#read 20 bytes from file
	fileFromServer = odb.get_object(res['ibm_cos']["bucket"], fileName, extra_get_args={'Range':'bytes={0}-{1}'.format(rang-20, rang)}).decode('UTF-8',errors='ignore')
	#Search an " " in the text
	while(fileFromServer[-1] != " "):
		fileFromServer = fileFromServer[:-1]
		rang = rang - 1
	return rang


def main(args):
	#get arguments
	s1 = json.dumps(args)
	args = json.loads(s1)
	res = args["res"]
	url = res["rabbitmq"]["url"]
	topRange = int(args["topRange"])
	bottomRange = int(args["bottomRange"])
	#configure COS library
	odb = COSBackend(res["ibm_cos"])
	
	#rabbitmq configuration
	params = pika.URLParameters(url)
	connection = pika.BlockingConnection(params)
	channel = connection.channel()
	channel.queue_declare(queue="CountingWords")

	#Calcules a range which doesn't cut any word
	#	if functionNumber = -1 it means that is the last one so it has to analyse until the end
	#	if functionNumber = 0 it means that is the 1st one and it can't search before it
	if args["functionNumber"] != "-1":
		topRange = selectRange(args["fileName"], topRange, res)
	if args["functionNumber"] != '0':
		bottomRange = selectRange(args["fileName"], bottomRange, res)

	#download the part of the file that is needed
	fileFromServer = odb.get_object(res["ibm_cos"]["bucket"], args["fileName"], extra_get_args={"Range": "bytes={0}-{1}".format(bottomRange, topRange)}).decode('UTF-8', errors='ignore')

	#Delete unwanted characters
	stringFiltered = re.sub('[^A-Za-z \n]+', '', fileFromServer)
	#Split the string
	stringSplitted = re.split("\ |\n", stringFiltered)
	#Delete "" in array
	stringSplitted = list(filter(None, stringSplitted))

	#create a json:
	#		{'words' : numberWords}
	body = json.dumps({"words":len(stringSplitted)})
	#send a msg to reduce function
	channel.basic_publish(exchange='', routing_key='CountingWords', body=body)
	#close connection
	connection.close()
	return {}
