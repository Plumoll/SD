import sys, yaml, time
from ibm_cf_connector import CloudFunctions
from cos_backend import COSBackend

#this function invokes the functionType given as an argument n times to IBM Cloud
#   param: 
#       function    -> functionType
#       nfunctions  -> times we want to invoke the function (CountingWords - WordCount)
#                                                           (reduce will only be invoked once)
#       fileSize    -> total size of the file we want to analyse
#       res         -> loaded configuration from ibm_cloud_config
def invokeFunctions(function, nFunctions, fileSize, fileName, res):
    topRange = 150000000
    #configure cloud function library
    cf = CloudFunctions(res['ibm_cf'])

    #range(0, nFunctions-1) since the last one needs to be invoked different
    for i in range(0, 10):
        #set the next function range
        

        #invoke the function with all its parameters
        #param -> res, filemame, bottom and top range, fileNumber
        print(topRange)
        _ = cf.invoke_with_result(function ,{"res": res, "fileName": fileName, "topRange": str(topRange), "bottomRange": str(0), "functionNumber":str(i)})
        print(_)
        topRange += 1000000
    

if __name__=='__main__':
    #read the arguments given
    fileName = sys.argv[1]
    nFunctions = int(sys.argv[2])

    #load config file
    with open('ibm_cloud_config', 'r') as config_file:
        res = yaml.safe_load(config_file)

    #configure COS library
    odb = COSBackend(res['ibm_cos'])
    #chech the total file of the file given as argument
    fileSize = int(odb.head_object(res['ibm_cos']["bucket"], fileName)["content-length"])

    start = time.time()
    #call the function nFunctions times and the reduce at the end
    invokeFunctions('wordCount', nFunctions,  fileSize, fileName, res)
    end1 = time.time()
    invokeFunctions('countingWords', nFunctions,  fileSize, fileName, res)
    end2 = time.time()
    print("wordCount function's time: {0}".format(end1 - start))
    print("CountingWords function's time: {0}".format(end2 - end1))