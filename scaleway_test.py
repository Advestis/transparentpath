# https://www.learnaws.org/2021/02/24/boto3-resource-client/
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#object

import boto3
AWS_ACCESS_KEY_ID = 'SCWSTECJS9J44EBXJX8K'
AWS_SECRET_ACCESS_KEY = '3a7e71d3-f5d7-48df-aee6-dc67d5500593'
AWS_DEFAULT_ACL = 'public-read'
AWS_S3_REGION_NAME = 'fr-par'
AWS_S3_ENDPOINT_URL = 'https://s3.fr-par.scw.cloud'

fs = boto3.resource(
    's3',
    region_name=AWS_S3_REGION_NAME,
    endpoint_url=AWS_S3_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
buckets = fs.buckets.all()
for b in buckets:
    print(b)

content = "chien"
obj = fs.Object("code-tests-sand", "dir1/dir2/newfile.txt")
obj.put(Body=content)
