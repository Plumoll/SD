import re
from cos_backend import COSBackend
import yaml
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
	counts = Counter()
	fileFromServer = odb.get_object(args["bucket"], args["fileName"], extra_get_args={'Range':args["rang"]}).decode('UTF-8')

	#stringFiltered = re.sub('[^A-Za-z0-9 \n]+', '', fileFromServer)
	stringFiltered = re.sub('[^A-Za-z \n]+', '', fileFromServer)
	stringSplitted = re.split("\ |\n", stringFiltered)
	stringSplitted.remove("")

	counts.update(word.strip('.,?!"\'').lower() for word in stringSplitted)
	return (dict(counts))