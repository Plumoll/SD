from ibm_cf_connector import CloudFunctions
import sys
import yaml
from cos_backend import COSBackend


if __name__=='__main__':
    filename=sys.argv[1]
    nFunctions=sys.argv[2]
    with open('ibm_cloud_config', 'r') as config_file:
        res = yaml.safe_load(config_file)
    #res['ibm_cos']['endpoint']
    cf = CloudFunctions(res['ibm_cf'])
    odb = COSBackend(res['ibm_cos'])
    filesize = int(odb.head_object("magisd", "bible.txt")["content-length"])
    rang = int(filesize/nFunctions)
    fileFromServer = odb.get_object("magisd", "bible.txt", extra_get_args={'Range':'bytes={0}-{1}'.format(rang-10, rang)}).decode('UTF-8')
    print(fileFromServer)

    _, rang = cutWord(fileFromServer, rang)

    print(rang)
    print(cf.invoke_with_result("countword", payload={'.format(0, rang)}))
    #fileFromServer = odb.get_object("magisd", "bible.txt", extra_get_args={'Range':'bytes={0}-{1}'.format(0, rang-2)}).decode('UTF-8')
    #print(fileFromServer)

    
def cutWord(fileFromServer, rang):
    if(fileFromServer[-1] != " "):
        cutWord(fileFromServer[:-1], rang-1)
    else:
        return fileFromServer, rang

    # while(fileFromServer[-1] != " "):
    #     fileFromServer = fileFromServer[:-1]
    #     print(fileFromServer)
    #     rang = rang - 1
    # return rang
