import yaml
from cos_backend import COSBackend


if __name__ == "__main__":
    with open('ibm_cloud_config', 'r') as config_file:
        res = yaml.safe_load(config_file)
    odb = COSBackend(res['ibm_cos'])
    fName = "gutenberg-1G.txt"

    f = open(fName,'rb') 

    odb.put_object("magisd", fName,f.read())

    f.close()
    print("done")