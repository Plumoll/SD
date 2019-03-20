from cos_backend import COSBackend


if __name__ == "__main__":
    odb = COSBackend()
    fName = "image123.jpeg"

    f = open(fName,'rb') 

    odb.put_object("magisd", fName,f.read())

    f.close()