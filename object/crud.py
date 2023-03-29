from urllib.request import urlopen
import io
import logging
from hashlib import md5
from time import localtime
from botocore.exceptions import ClientError
import os

def get_objects(aws_s3_client, bucket_name) -> str:
  for key in aws_s3_client.list_objects(Bucket=bucket_name)['Contents']:
    print(f" {key['Key']}, size: {key['Size']}")


def download_file_and_upload_to_s3(aws_s3_client,
                                   bucket_name,
                                   url,
                                   keep_local=False) -> str:
  file_name = f'image_file_{md5(str(localtime()).encode("utf-8")).hexdigest()}.jpg'
  with urlopen(url) as response:
    content = response.read()
    aws_s3_client.upload_fileobj(Fileobj=io.BytesIO(content),
                                 Bucket=bucket_name,
                                 ExtraArgs={'ContentType': 'image/jpg'},
                                 Key=file_name)
  if keep_local:
    with open(file_name, mode='wb') as jpg_file:
      jpg_file.write(content)

  # public URL
  return "https://s3-{0}.amazonaws.com/{1}/{2}".format('us-west-2',
                                                       bucket_name, file_name)


# def upload_file(aws_s3_client, filename, bucket_name):
#   response = aws_s3_client.upload_file(filename, bucket_name, "hello.txt")
#   status_code = response["ResponseMetadata"]["HTTPStatusCode"]
#   if status_code == 200:
#     return True
#   return False


def upload_file_obj(aws_s3_client, filename, bucket_name):
  with open(filename, "rb") as file:
    aws_s3_client.upload_fileobj(file, bucket_name, "hello_obj.txt")


def upload_file_put(aws_s3_client, filename, bucket_name):
  with open(filename, "rb") as file:
    aws_s3_client.put_object(Bucket=bucket_name,
                             Key="hello_put.txt",
                             Body=file.read())

# def upload_small_size_file(aws_s3_client, file_path, bucket_name, object_name=None):
#     if object_name is None:
#         object_name = file_path
#     with open(file_path, "rb") as file:
#       response = aws_s3_client.upload_fileobj(file, bucket_name, object_name)
#       status_code = response["ResponseMetadata"]["HTTPStatusCode"]

#     if status_code == 200:
#         print(f"Sucessfully upload file {file_path} to S3 bucket {bucket_name}")
#     else:
#         print(f"Failed to upload file {file_path} to S3 bucket {bucket_name}")

def upload_small_size_file(aws_s3_client, file_name, bucket_name, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)
    try:
        response = aws_s3_client.upload_file(file_name, bucket_name, object_name)
        # status_code = response["ResponseMetadata"]["HTTPStatusCode"]

        
    except ClientError as e:
        logging.error(e)
        return False
    return True