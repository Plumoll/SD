from ibm_cf_connector import CloudFunctions
import sys, yaml, time
from cos_backend import COSBackend


#this function calcules a topRange which doesn't cut any word
#   comments:
#       it starts searching from the end and when it finds an space returns the value
#       the range of data downloaded for this is 20 since the average words lenth is more or less 5 so it will have an space always
#   param:
#       fileName    ->  name of the file
#       rang        ->  rang from which we want to analyse
#       res         ->  loaded configuration from ibm_cloud_config
def selectRange(fileName, rang, res):
    #configure COS library
    odb = COSBackend(res['ibm_cos'])
    fileFromServer = odb.get_object(res['ibm_cos']["bucket"], fileName, extra_get_args={'Range':'bytes={0}-{1}'.format(rang-20, rang)}).decode('UTF-8',errors='ignore')
    while(fileFromServer[-1] != " "):
        fileFromServer = fileFromServer[:-1]
        rang = rang - 1
    return rang

#this function invokes the functionType given as an argument n times to IBM Cloud
#   param: 
#       function    -> functionType
#       nfunctions  -> times we want to invoke the function (CountingWords - WordCount)
#                                                           (reduce will only be invoked once)
#       fileSize    -> total size of the file we want to analyse
#       res         -> loaded configuration from ibm_cloud_config
def invokeFunctions(function, nFunctions, fileSize, fileName, res):
    #last invoked function's topRange
    bottomRange = 0
    #configure cloud function library
    cf = CloudFunctions(res['ibm_cf'])
    #loop invoking functions
    #range(0, nFunctions-1) since the last one needs to be invoked different

    for i in range(0, nFunctions-1):
        #fileSize/nFunctions as every function will operate with more or less the same amount of data
        #bottomRange to not give the same data as the previous one
        topRange = int(fileSize/nFunctions) + bottomRange
        #avoid any cut words
        topRange = selectRange(fileName, topRange, res)
        #invoke the function with all its parameters
        #param -> res, filemame, bottom and top range, fileNumber
        cf.invoke(function ,{"res": res, "fileName": fileName, "rang": "bytes={0}-{1}".format(bottomRange, topRange), "functionNumber":str(i)})
        #update the bottomRange
        bottomRange = topRange
    
    #same as the previous ones but topRange is replaced for fileSize
    cf.invoke(function ,{"res": res, "fileName": fileName, "rang": "bytes={0}-{1}".format(bottomRange, fileSize), "functionNumber":str(i)})

    #invoke the reduce function(recieves messages from the other ones)
    #invoke with result is necessary here as we want the total needed to finish.
    #param -> res, nIterations, fileName
    _ = cf.invoke_with_result("reduce" ,{"res": res, "iter":nFunctions, "fileName":fileName})
    
if __name__=='__main__':
    #read the arguments given
    fileName = sys.argv[1]
    nFunctions = int(sys.argv[2])
    functionType = sys.argv[3]
    #load config file
    with open('ibm_cloud_config', 'r') as config_file:
        res = yaml.safe_load(config_file)
    #configure COS library
    odb = COSBackend(res['ibm_cos'])
    #chech the total file of the file given as argument
    fileSize = int(odb.head_object(res['ibm_cos']["bucket"], fileName)["content-length"])
    #start clock
    start = time.time()
    #call the function nFunctions times and the reduce at the end
    invokeFunctions(functionType, nFunctions,  fileSize, fileName, res)
    #finish clock
    end = time.time()
    print(end-start)  

    #! for me
    #  cf.invoke(function ,{"bucket": res['ibm_cos']["bucket"], "fileName": fileName, "rang": "bytes={0}-{1}".format(bottomRange, fileSize), "endpoint":res['ibm_cos']["endpoint"],
    #                         "access_key":res['ibm_cos']["access_key"], "secret_key":res['ibm_cos']["secret_key"], "url":res["rabbitmq"]["url"],
    #                          "functionNumber":str(nFunctions)})
# _ = cf.invoke_with_result("reduce" ,{"bucket": res['ibm_cos']["bucket"], "endpoint":res['ibm_cos']["endpoint"],
#                             "access_key":res['ibm_cos']["access_key"], "secret_key":res['ibm_cos']["secret_key"], "url":res["rabbitmq"]["url"],
#                              "iter":nFunctions, "fileName":fileName})
