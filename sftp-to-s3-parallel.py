import os
import paramiko
import sys
import io
import math
import time
import boto3
import logging
from stat import S_ISDIR, S_ISREG
from datetime import timedelta, datetime
import multiprocessing
from multiprocessing import Pool
import json


root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)
root.info("check")



import json

def get_secret():

    secret_name = "sftp"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager'
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    return secret
            
    # Your code goes here. 



def open_ftp_connection(ftp_host, ftp_port, ftp_username, ftp_password):
    '''
    Opens ftp connection and returns connection object
    '''
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    try:
        transport = paramiko.Transport(ftp_host, ftp_port)
    except Exception as e:
        return 'conn_error'
    try:
        transport.connect(username=ftp_username, password=ftp_password)
    except Exception as identifier:
        return 'auth_error'
    ftp_connection = paramiko.SFTPClient.from_transport(transport)
    return ftp_connection

def transfer_chunk_from_ftp_to_s3(
        ftp_file, 
        s3_connection, 
        multipart_upload, 
        bucket_name, 
        ftp_file_path, 
        s3_file_path, 
        part_number, 
        chunk_size
    ):
    start_time = time.time()
    chunk = ftp_file.read(int(chunk_size))
    part = s3_connection.upload_part(
        Bucket = bucket_name,
        Key = s3_file_path,
        PartNumber = part_number,
        UploadId = multipart_upload['UploadId'],
        Body = chunk,
    )
    end_time = time.time()
    total_seconds = end_time - start_time
    root.info('speed is {} kb/s total seconds taken {}'.format(math.ceil((int(chunk_size) /1024) / total_seconds), total_seconds))
    part_output = {
        'PartNumber': part_number,
        'ETag': part['ETag']
    }
    return part_output

def transfer_file_from_ftp_to_s3(bucket_name, ftp_file_path, ftp_username, ftp_password, chunk_size):
    s3_file_path = S3_PATH + ftp_file_path
    s3_file_path = s3_file_path.replace('//', '/')
    ftp_connection = open_ftp_connection(FTP_HOST, int(FTP_PORT), ftp_username, ftp_password) 
    ftp_file = ftp_connection.file(ftp_file_path, 'r')
    s3_connection = boto3.client('s3')
    ftp_file_size = ftp_file._get_size()
    try:
        root.info('Looking for file %s in s3... checking if s3 file size match with ftp...', ftp_file_path)
        s3_file = s3_connection.head_object(Bucket = bucket_name, Key = s3_file_path)
        if s3_file['ContentLength'] == ftp_file_size:
            root.info('File Already Exists in S3 bucket')
            ftp_file.close()
            return
    except Exception as e:
        pass
    if ftp_file_size <= int(chunk_size):
        root.info('Start copy of ftp file %s to s3 as %s' % (ftp_file_path , s3_file_path))
        #upload file in one go
        root.info('Transferring complete File from FTP to S3...')
        ftp_file_data = ftp_file.read()
        s3_connection.upload_fileobj(io.BytesIO(ftp_file_data), bucket_name, s3_file_path)
        root.info('Successfully Transferred file from FTP to S3!')
        ftp_file.close()

    else:
        root.info('Start copy of ftp file %s to s3 as %s' % (ftp_file_path , s3_file_path))
        root.info('Transferring File from FTP to S3 in chunks...')
        #upload file in chunks
        chunk_count = int(math.ceil(ftp_file_size / float(chunk_size)))
        multipart_upload = s3_connection.create_multipart_upload(Bucket = bucket_name, Key = s3_file_path)
        parts = []
        for  i in range(chunk_count):
            root.info('Transferring chunk {}...'.format(i + 1))
            part = transfer_chunk_from_ftp_to_s3(
                ftp_file, 
                s3_connection, 
                multipart_upload, 
                bucket_name, 
                ftp_file_path, 
                s3_file_path.replace('//', '/'), 
                i + 1, 
                chunk_size
            )
            parts.append(part)
            root.info('Chunk {} Transferred Successfully!'.format(i + 1))

        part_info = {
            'Parts': parts
        }
        s3_connection.complete_multipart_upload(
        Bucket = bucket_name,
        Key = s3_file_path.replace('//', '/'),
        UploadId = multipart_upload['UploadId'],
        MultipartUpload = part_info
        )
        root.info('All chunks Transferred to S3 bucket! File Transfer successful!')
        ftp_file.close()


lst = []
def listdir_r(ftp_connection, FTP_PATH):
    for entry in ftp_connection.listdir_attr(FTP_PATH):
        remotepath = FTP_PATH + "/" + entry.filename
        mode = entry.st_mode
        if S_ISDIR(mode):
            listdir_r(ftp_connection, remotepath.replace('//', '/'))
        elif S_ISREG(mode):
            # We only care about files in the last `DAYS_BACK` days                    
            if entry.st_mtime < (datetime.today() - timedelta(days=DAYS_BACK)).timestamp():
                root.info('Skipping file %s, due last modification date. Rule is DAYS_BACK = %s ' % (entry,DAYS_BACK) )
                continue
            lst.append(remotepath)



if __name__ == '__main__':
    conf = json.loads(get_secret())
    S3_BUCKET_NAME = conf["S3_BUCKET_NAME"]
    S3_PATH = conf["S3_PATH"]
    FTP_HOST = conf["FTP_HOST"]
    FTP_PORT = int(conf["FTP_PORT"])
    FTP_USERNAME = conf["FTP_USERNAME"]
    FTP_PASSWORD = conf["FTP_PASSWORD"]
    FTP_PATH = conf["FTP_PATH"]
    DAYS_BACK = int(conf["DAYS_BACK"])
    CHUNK_SIZE = int(conf["CHUNK_SIZE"])

    ftp_username = FTP_USERNAME
    ftp_password = FTP_PASSWORD
    ftp_connection = open_ftp_connection(FTP_HOST, int(FTP_PORT), ftp_username, ftp_password)
    if ftp_connection == 'conn_error':
        root.info('Failed to connect FTP Server!')
    elif ftp_connection == 'auth_error':
        root.info('Incorrect username or password!')
    else:
        root.info("Creating list for sftp files...")
        
        num_cores = 8
        listdir_r (ftp_connection, FTP_PATH)
        p = multiprocessing.Pool(processes = num_cores)
        start = time.time()
        for i in lst:
            p.apply_async(transfer_file_from_ftp_to_s3, [S3_BUCKET_NAME, i, ftp_username, ftp_password, CHUNK_SIZE])
        p.close()
        p.join()
        root.info("Complete")
        end = time.time()
        root.info('total time (s)= ' + str(end-start))
