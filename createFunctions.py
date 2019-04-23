import shutil, yaml, os
from ibm_cf_connector import CloudFunctions

if __name__ == '__main__':
    #create zip files
    shutil.make_archive('reduce', 'zip', 'Reduce')
    shutil.make_archive('countingWords', 'zip', 'CountingWords')
    shutil.make_archive('wordCount', 'zip', 'WordCount')

    #load ibm_cloud_config file
    with open('ibm_cloud_config', 'r') as config_file:
        res = yaml.safe_load(config_file)
    #configure cloud function library
    cf = CloudFunctions(res['ibm_cf'])

    #create new actions, they are updated in case of being created already
    f = open('reduce.zip','rb')
    cf.create_action('reduce', f.read(), kind='python:3.7')

    f = open('wordCount.zip','rb')
    cf.create_action('wordCount', f.read(), kind='python:3.7')

    f = open('countingWords.zip','rb')
    cf.create_action('countingWords', f.read(), kind='python:3.7')

    #delete zip files
    os.remove('reduce.zip')
    os.remove('countingWords.zip')
    os.remove('wordCount.zip')

