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
    bottomRange = 0
    #configure cloud function library
    cf = CloudFunctions(res['ibm_cf'])

    #range(0, nFunctions-1) since the last one needs to be invoked different
    for i in range(0, nFunctions-1):
        #set the next function range
        topRange = int(fileSize/nFunctions) + bottomRange

        #invoke the function with all its parameters
        #param -> res, filemame, bottom and top range, fileNumber
        cf.invoke(function ,{"res": res, "fileName": fileName, "topRange": str(topRange), "bottomRange": str(bottomRange), "functionNumber":str(i)})
        
        bottomRange = topRange
    
    #same as the previous ones but topRange is replaced for fileSize
    cf.invoke(function ,{"res": res, "fileName": fileName, "topRange": str(fileSize), "bottomRange": str(bottomRange), "functionNumber":str(-1)})

    #invoke the reduce function(recieves messages from the other ones)
    #invoke with result as we want the time needed to finish.
    #  param -> res, nIterations, fileName
    _ = cf.invoke_with_result("reduce" ,{"res": res, "iter":nFunctions, "fileName":fileName})




if __name__=='__main__':
    #read the arguments given
    fileName = sys.argv[1]
    nFunctions = 1
    #load config file
    with open('../ibm_cloud_config', 'r') as config_file:
        res = yaml.safe_load(config_file)

    #configure COS library
    odb = COSBackend(res['ibm_cos'])

    fileSize = int(odb.head_object(res['ibm_cos']["bucket"], fileName)["content-length"])
    #check if there are enough workers

    while(fileSize / nFunctions) > 110000000: #this number is an aproximation i don't know the limit
        nFunctions +=1

    for i in (1,2,3,4,5,7,10,12,15,20,25,30,35,40,45,50):
    #invoke functions calculating the time
        if(i>=nFunctions):
            timeWordCount = 0
            timeCountingWords = 0
            for _ in range(0,4):    #average
                start = time.time()
                invokeFunctions('wordCount', i,  fileSize, fileName, res)
                end1 = time.time()
                invokeFunctions('countingWords', i,  fileSize, fileName, res)
                end2 = time.time()
                
                timeWordCount += (end1 - start)
                timeCountingWords += (end2 - end1)

            print("WorkersUsed: {}".format(i))
            print("wordCount function's time: {0}".format(timeWordCount/4))
            print("CountingWords function's time: {0}".format(timeCountingWords/4))

    #download generated files
    # fileFromServer = odb.get_object(res['ibm_cos']["bucket"], fileName[:-4] + 'CountingWordResult.txt')
    # newFile = open(fileName[:-4] + 'CountingWordResult.txt', "wb")
    # newFile.write(fileFromServer)
    # newFile.close()
    # print(fileName[:-4] + 'CountingWordResult.txt downloaded')

    # fileFromServer = odb.get_object(res['ibm_cos']["bucket"], fileName[:-4] + 'WordCountResult.txt')
    # newFile = open(fileName[:-4] + 'WordCountResult.txt', "wb")
    # newFile.write(fileFromServer)
    # newFile.close()
    # print(fileName[:-4] + 'WordCountResult.txt downloaded')