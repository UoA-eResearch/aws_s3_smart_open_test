#!/usr/bin/python3
import os
import sys
import boto3
import botocore
from smart_open import open
import re
import hashlib
import codecs
import argparse
import json

class S3RSync:
  #Temporary file used in the S3 copy. Needed only while the concurrent S3 session bug exists.
  TEMP_FILENAME = '/tmp/s3_tmp'
  CHUNK_SIZE = 1073741824 #1G

  def __init__(self, src_keys, src_endpoint, dest_keys, dest_endpoint, chunk_size=CHUNK_SIZE, debug = 0, update = True):
    '''
      Cache s3 credentials for later use.
      
      src_keys:      S3 key ID and secret of the source S3 server.
      src_endpoint:  URI for the source S3 server
      dest_keys:     S3 key ID and secret for the destination S3 server
      dest_endpoint: URI for the destination S3 server
      self.chunk_size:    Read/write buffer size. Affects the S3 ETag, when more data size > chunk size.
    '''

    self.src_session = boto3.Session(
         aws_access_key_id=src_keys['access_key_id'],
         aws_secret_access_key=src_keys['secret_access_key']
    )
    self.src_connection = self.src_session.client(
        's3',
        aws_session_token=None,
        region_name='us-east-1',
        use_ssl=True,
        endpoint_url=src_endpoint,
        config=None
    )
    self.src_endpoint = src_endpoint

    
    self.chunk_size = chunk_size
    self.debug = debug
    self.update = update

  def read_in_chunks(self, file_object):
    '''
      Iterator to read a file chunk by chunk.
    
      file_object: file opened by caller
    '''
    while True:
      data = file_object.read(self.chunk_size)
      if not data:
        break
      yield data

  def etag(self, md5_array):
    ''' 
      Calculate objects ETag from array of chunk's MD5 sums
    
      md5_array: md5 hash of each buffer read
    '''
    if len(md5_array) < 1:
      return '"{}"'.format(hashlib.md5().hexdigest())

    if len(md5_array) == 1:
      return '"{}"'.format(md5_array[0].hexdigest())

    digests = b''.join(m.digest() for m in md5_array)
    digests_md5 = hashlib.md5(digests)
    return '"{}-{}"'.format(digests_md5.hexdigest(), len(md5_array))

  def cat_obj_mytardis(self, key, bucket, prefix = None):
    #Read from object and print
    object_name = os.path.basename(key) if prefix is None else '{}/{}'.format(prefix, os.path.basename(key))
    source_s3_uri = 's3://{}/{}'.format(bucket, object_name)
    multipart = False

    with open(source_s3_uri, 'r', transport_params={'session': self.src_session,  'buffer_size': self.chunk_size, 'resource_kwargs': { 'endpoint_url': self.src_endpoint}}, ignore_ext=True) as s3_source:
      for chunk in self.read_in_chunks(s3_source):
        print(chunk)

  def copy_file_mytardis(self, filename, bucket, prefix = None):
    #Test of write to source S3
    #Read from file; Write to src S3.
    object_name = os.path.basename(filename) if prefix is None else '{}/{}'.format(prefix, os.path.basename(filename))
    source_s3_uri = 's3://{}/{}'.format(bucket, object_name)
    multipart = False

    with open(filename, 'rb') as fin:
      with open(source_s3_uri, 'wb', transport_params={'session': self.src_session,  'buffer_size': self.chunk_size, 'resource_kwargs': { 'endpoint_url': self.src_endpoint}, 'multipart_upload': multipart}, ignore_ext=True) as s3_destination:
        for chunk in self.read_in_chunks(fin):
           s3_destination.write(chunk)


def json_load(filename):
  try:
    with open( filename ) as f:
      return json.load(f)
  except Exception as e:
    print( "json_load({}): ".format(filename), e )
    sys.exit(1)

def main():
  auth = json_load(os.path.dirname(os.path.realpath(__file__)) + '/conf/test_auth.json')
  conf = json_load(os.path.dirname(os.path.realpath(__file__)) + '/conf/test_conf.json')

  s3rsync = S3RSync(src_keys=auth['src_s3_keys'], src_endpoint=auth['src_endpoint'], dest_keys=auth['dest_s3_keys'], dest_endpoint=auth['dest_endpoint'])

  filename = os.path.dirname(os.path.realpath(__file__)) + '/data/Snark.txt'
  s3rsync.copy_file_mytardis(filename = filename, bucket = 'mytardis', prefix='Write_Test')
  
if __name__ == "__main__":
  main()
