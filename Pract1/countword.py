import re
from cos_backend import COSBackend


if __name__ == "__main__":
	odb = COSBackend()

	fileFromServer = odb.get_object("magisd", "bible.txt", extra_get_args={'Range':'bytes=0-100000'}).decode('UTF-8')

	newFile = open("newBible.txt", "w")
	stringFiltered = re.sub('[^A-Za-z0-9 \n]+', '', fileFromServer)
	stringSplitted = re.split("\ |\n", stringFiltered)
	wordDictionary = {}
	for word in stringSplitted:
		lowerWord = word.lower()
		if lowerWord != '':
			if lowerWord in wordDictionary:
				wordDictionary[lowerWord] = wordDictionary[lowerWord] + 1
			else:
				wordDictionary[lowerWord] = 1
	
	print(wordDictionary)
	newFile.write(fileFromServer)

	newFile.close()