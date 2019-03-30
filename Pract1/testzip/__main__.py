import re
from cos_backend import COSBackend
import yaml
import sys
from collections import Counter
import json

def main(args):
	with open('ibm_cloud_config', 'r') as config_file:
		res = yaml.safe_load(config_file)
		#res['ibm_cos']['endpoint']
	odb = COSBackend(res['ibm_cos'])
	print(args)
	s1 = json.dumps(args)
	args = json.loads(s1)
	fileName = args["fileName"]
	bucket = args["bucket"]
	rang = args["rang"]
	counts = Counter()
	fileFromServer = odb.get_object(bucket, fileName, extra_get_args={'Range':rang}).decode('UTF-8')

	#newFile = open("newBible.txt", "w")
	stringFiltered = re.sub('[^A-Za-z0-9 \n]+', '', fileFromServer)
	stringFiltered = re.sub('[^A-Za-z \n]+', '', fileFromServer)
	stringSplitted = re.split("\ |\n", stringFiltered)
	stringSplitted.remove("")

	counts.update(word.strip('.,?!"\'').lower() for word in stringSplitted)
	print(dict(counts))
	return (dict(counts))
# if __name__ == "__main__":
# 	print(main())
#fileFromServer = odb.get_object("magisd", "bible.txt", extra_get_args={'Range':'bytes=0-100'}).decode('UTF-8')

