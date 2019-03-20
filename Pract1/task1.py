import re

pathFile = 'file.txt'
file = open(pathFile, 'r')
i = 0
for line in file:
    re.sub('[^A-Za-z0-9 ]+', '', line)

