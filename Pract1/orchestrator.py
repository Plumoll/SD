from ibm_cf_connector import CloudFunctions
import sys
import yaml
from cos_backend import COSBackend

def selectRange(fileFromServer, rang):
    while(fileFromServer[-1] != " "):
        fileFromServer = fileFromServer[:-1]
        rang = rang - 1
    return rang
    
if __name__=='__main__':
    filename=sys.argv[1]
    nFunctions=int(sys.argv[2])
    with open('ibm_cloud_config', 'r') as config_file:
        res = yaml.safe_load(config_file)
    cf = CloudFunctions(res['ibm_cf'])
    odb = COSBackend(res['ibm_cos'])
    filesize = int(odb.head_object("magisd", filename)["content-length"])
    bottomRang = 0

    for i in range(0, nFunctions-1):
        topRang = int(filesize/nFunctions) + bottomRang
        fileFromServer = odb.get_object("magisd", filename, extra_get_args={'Range':'bytes={0}-{1}'.format(topRang-20, topRang)}).decode('UTF-8')
        topRang = selectRange(fileFromServer, topRang)
        
        #call function
        cf.invoke("pikachu" ,{"bucket": "magisd", "fileName": filename, "rang": "bytes={0}-{1}".format(bottomRang, topRang), "endpoint":res['ibm_cos']["endpoint"],
                            "access_key":res['ibm_cos']["access_key"], "secret_key":res['ibm_cos']["secret_key"], "url":"amqp://xbjymxoa:jdlKHnEzsJ3woxT8wHGtox-8PI7kJXwW@caterpillar.rmq.cloudamqp.com/xbjymxoa"})
        
        bottomRang = topRang


    cf.invoke("pikachu" ,{"bucket": "magisd", "fileName": filename, "rang": "bytes={0}-{1}".format(bottomRang, filesize), "endpoint":res['ibm_cos']["endpoint"],
                            "access_key":res['ibm_cos']["access_key"], "secret_key":res['ibm_cos']["secret_key"], "url":"amqp://xbjymxoa:jdlKHnEzsJ3woxT8wHGtox-8PI7kJXwW@caterpillar.rmq.cloudamqp.com/xbjymxoa"})
        

    print(filesize)