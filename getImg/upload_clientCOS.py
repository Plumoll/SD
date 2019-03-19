from cos_backend import COSBackend



odb = COSBackend()

fName = str(raw_input("Quin fitxer vols pujar? "))

print fName
fName = "image123.jpeg"

f = open(fName, "r")

odb.put_object("galleda-d-aigua", fName,f.read())

f.close()
