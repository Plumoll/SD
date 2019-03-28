import re
from cos_backend import COSBackend
import yaml
import sys
from collections import Counter


def main(args):
	with open('ibm_cloud_config', 'r') as config_file:
		res = yaml.safe_load(config_file)
    #res['ibm_cos']['endpoint']
	odb = COSBackend(res['ibm_cos'])
	counts = Counter()
	fileFromServer = odb.get_object("magisd", "bible.txt", extra_get_args={'Range':'bytes=0-100'}).decode('UTF-8')

	#newFile = open("newBible.txt", "w")
	#stringFiltered = re.sub('[^A-Za-z0-9 \n]+', '', fileFromServer)
	stringFiltered = re.sub('[^A-Za-z \n]+', '', fileFromServer)
	stringSplitted = re.split("\ |\n", stringFiltered)
	stringSplitted.remove("")

	counts.update(word.strip('.,?!"\'').lower() for word in stringSplitted)
	
	#print(wordDictionary)
	#newFile.write(fileFromServer)
	return dict(counts)
	

	#newFile.close()

# if __name__ == "__main__":
# 	print(main())