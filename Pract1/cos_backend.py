import ibm_boto3
import ibm_botocore
import json


class COSBackend:
    def __init__ (self):
        f = open('../credentials.txt', 'r')
        credentials = json.loads(f.read())
        service_endpoint = credentials.get("service_endpoint")
        secret_key = credentials.get("secret_key")
        access_key = credentials.get("access_key")
        client_config = ibm_botocore.client.Config(max_pool_connections=200)

        self.cos_client = ibm_boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=client_config,
            endpoint_url=service_endpoint
        )


    def put_object(self, bucket_name, key, data):
        self.cos_client.put_object(Bucket=bucket_name, Key=key, Body=data)

    def get_object(self, bucket_name, key, stream=False, extra_get_args={}):
        r = self.cos_client.get_object(Bucket=bucket_name, Key=key, **extra_get_args)
        if stream:
            data = r['Body']
        else:
            data = r['Body'].read()
        return data

    def head_object(self, bucket_name, key):
        metadata = self.cos_client.head_object(Bucket=bucket_name, Key=key)
        return metadata['ResponseMetadata']['HTTPHeaders']

    def delete_object(self, bucket_name, key):
        return self.cos_client.delete_object(Bucket=bucket_name, Key=key)
