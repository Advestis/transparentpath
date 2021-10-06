# https://www.learnaws.org/2021/02/24/boto3-resource-client/
# https://living-sun.com/fr/python/727389-s3fs-python-credential-inline-python-amazon-web-services-s3fs.html

import s3fs

AWS_ACCESS_KEY_ID = "SCWSTECJS9J44EBXJX8K"
AWS_SECRET_ACCESS_KEY = "3a7e71d3-f5d7-48df-aee6-dc67d5500593"
AWS_DEFAULT_ACL = "public-read"
AWS_S3_REGION_NAME = "fr-par"
AWS_S3_ENDPOINT_URL = "https://s3.fr-par.scw.cloud"
token = "/home/pcotte/scaleway_token.json"

fs = s3fs.S3FileSystem(
    anon=False,
    token=token,
    client_kwargs={
        "endpoint_url": AWS_S3_ENDPOINT_URL
    },
    asynchronous=False
)
print(fs.exists("s3://code-tests-sand/dir1"))
