import sys
from cos_backend import COSBackend
import json

def main(args):
	odb = COSBackend()
	#print(args)
	#print("----"+args["json"])
	args = args["json"]
	fDown = args["retrieve"]
	bucket = args["from"]
	fName = args["as"]

	fileFromServer = odb.get_object(bucket, fDown)

	newFile = open(fName, "w")

	newFile.write(fileFromServer)

	newFile.close()

	return {}

if __name__ == "__main__":
	main(sys.argv[1])
