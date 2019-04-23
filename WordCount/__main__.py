import re, json, pika
from cos_backend import COSBackend
from collections import Counter

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

	counts = Counter()
	
	#pika configuration
	params = pika.URLParameters(url)
	connection = pika.BlockingConnection(params)
	channel = connection.channel()
	channel.queue_declare(queue='WordCount')

	#Calcules a range which doesn't cut any word
	#	if functionNumber = -1 means that is the last one so it has to analyse until the end
	#	if functionNumber = 0 means that is the 1st one and it can't search before it
	if args["functionNumber"] != "-1":
		topRange = selectRange(args["fileName"], topRange, res)
	if args["functionNumber"] != '0':
		bottomRange = selectRange(args["fileName"], bottomRange, res)

	#get the part of the file that is needed in this function
	fileFromServer = odb.get_object(res["ibm_cos"]["bucket"], args["fileName"], extra_get_args={"Range": "bytes={0}-{1}".format(bottomRange, topRange)}).decode('UTF-8', errors='ignore')

	#Delete unwanted characters
	stringSplitted = re.sub('[^A-Za-z \n]+', '', fileFromServer)
	#Split the string
	stringSplitted = re.split("\ |\n", stringSplitted)
	#Delete "" in array
	stringSplitted = list(filter(None, stringSplitted))

	#convert array to count:
	#	{word1:numberWord1, word2:numberWord2...wordN:numberWordN}
	counts.update(word.strip('.,?!"\'').lower() for word in stringSplitted)
	#count to dict
	diccionary = dict(counts)
	#dict to json
	dumped_json_string = json.dumps(diccionary)
	
	#upload file with result:
	#	nameFile	->	book + numberFunction
	#	body		->	json(dict(count))
	odb.put_object(res["ibm_cos"]["bucket"], args["fileName"]+args["functionNumber"], dumped_json_string)
	#send a msg to reduce with the file name as body
	channel.basic_publish(exchange='', routing_key='WordCount', body=args["fileName"]+args["functionNumber"])
	#close the connection
	connection.close()
	return {}
