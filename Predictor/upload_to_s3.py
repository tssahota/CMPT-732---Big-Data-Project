import boto3
from botocore.exceptions import ClientError
import os
import sys


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def uploadDirectory(source, target, bucketname):
    s3_client = boto3.client('s3')
    #folder_name = target
    #try:
     #   s3.put_object(Bucket=bucketname, Key=(folder_name+'/'))
    #except Exception as e:
     #   print(e)
    #bucketname = bucketname+'/'+folder_name+'/'
    for root,dirs,files in os.walk(source):
        for file in files:
            try:
                response = s3_client.upload_file(os.path.join(root,file),bucketname,file)
            except ClientError as e:
                logging.error(e)
                print("Error uploading : "+file)

if __name__ == '__main__':
    source = sys.argv[1]
    target = sys.argv[2]
    bucket = sys.argv[3]
    is_dir = int(sys.argv[4])
    #bucket = 'sagemaker-studio-lrp9p38z5a9'
    if is_dir == 0:
        print("uploading")
        uploaded = upload_file(source, bucket, object_name=target)
    else: 
        uploadDirectory(source, target, bucket)

