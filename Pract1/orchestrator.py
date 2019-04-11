from ibm_cf_connector import CloudFunctions
import sys, yaml, time
from cos_backend import COSBackend

def selectRange(fileFromServer, rang):
    while(fileFromServer[-1] != " "):
        fileFromServer = fileFromServer[:-1]
        rang = rang - 1
    return rang


def invokeFunctions(function, nFunctions, fileSize, fileName, res, cf):
    bottomRang = 0
    for i in range(0, nFunctions-1):
        topRang = int(fileSize/nFunctions) + bottomRang
        fileFromServer = odb.get_object("magisd", fileName, extra_get_args={'Range':'bytes={0}-{1}'.format(topRang-20, topRang)}).decode('UTF-8',errors='ignore')
        topRang = selectRange(fileFromServer, topRang)
        cf.invoke(function ,{"bucket": res['ibm_cos']["bucket"], "fileName": fileName, "rang": "bytes={0}-{1}".format(bottomRang, topRang), "endpoint":res['ibm_cos']["endpoint"],
                             "access_key":res['ibm_cos']["access_key"], "secret_key":res['ibm_cos']["secret_key"], "url":res["rabbitmq"]["url"],
                             "functionNumber":str(i)})
        bottomRang = topRang

    cf.invoke(function ,{"bucket": res['ibm_cos']["bucket"], "fileName": fileName, "rang": "bytes={0}-{1}".format(bottomRang, fileSize), "endpoint":res['ibm_cos']["endpoint"],
                            "access_key":res['ibm_cos']["access_key"], "secret_key":res['ibm_cos']["secret_key"], "url":res["rabbitmq"]["url"],
                             "functionNumber":str(nFunctions)})

    _ = cf.invoke_with_result("reduce" ,{"bucket": res['ibm_cos']["bucket"], "endpoint":res['ibm_cos']["endpoint"],
                            "access_key":res['ibm_cos']["access_key"], "secret_key":res['ibm_cos']["secret_key"], "url":res["rabbitmq"]["url"],
                             "iter":nFunctions})
    
if __name__=='__main__':
    fileName=sys.argv[1]
    nFunctions=int(sys.argv[2])
    with open('ibm_cloud_config', 'r') as config_file:
        res = yaml.safe_load(config_file)
    cf = CloudFunctions(res['ibm_cf'])
    odb = COSBackend(res['ibm_cos'])
    fileSize = int(odb.head_object("magisd", fileName)["content-length"])

    start = time.time()
    invokeFunctions(sys.argv[3], nFunctions,  fileSize, fileName, res, cf)
    end = time.time()
    print(end-start)  
