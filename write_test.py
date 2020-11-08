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
  #CHUNK_SIZE = 1073741824 #1G
  CHUNK_SIZE = 67108864 #64M (50*1024*204 minimum for smart_open (5M for S3))
  
  def __init__(self, s3_keys, s3_endpoint, chunk_size=CHUNK_SIZE, debug = 0, update = True):
    '''
      Cache s3 credentials for later use.
      
      src_keys:      S3 key ID and secret of the source S3 server.
      s3_endpoint:  URI for the source S3 server
      self.chunk_size:    Read/write buffer size. Affects the S3 ETag, when more data size > chunk size.
      debug:        Debug output increases with this value
      update:       Only write if this is True, otherwise only run through actions, printing debug info.
    '''

    self.s3_session = boto3.Session(
         aws_access_key_id=s3_keys['access_key_id'],
         aws_secret_access_key=s3_keys['secret_access_key']
    )
    self.s3_connection = self.s3_session.client(
        's3',
        aws_session_token=None,
        region_name='us-east-1',
        use_ssl=True,
        endpoint_url=s3_endpoint,
        config=None
    )
    self.s3_endpoint = s3_endpoint

    
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

  def s3copyfile(self, filename, bucket, key, prefix = None, disable_multipart = False):
    ''' 
      S3 copy from source S3 object to destination s3 object ( renamed as bucket/original_object_name )
      
      source_file: Name of file to copy to S3
      bucket: destination S3 bucket to copy the object to
      key:         Object name
      size:        length of object, in bytes. Used to determine if we write in chunks, or not (which affects the ETag created)
      prefix:      Change key to 'prefix/key'
      disable_multipart Don't compare the file size with the chunk size to determine if this should be a multipart upload.
    '''
    
    if prefix is not None:
      key = '{}/{}'.format(prefix, key)
    s3_uri = 's3://{}/{}'.format(bucket, key)
    
    size = os.path.getsize(filename)

    multipart = size > self.chunk_size
    if disable_multipart: multipart = False #Might have an original ETag, that wasn't multipart, but bigger than than the chunk_size.

    #Read from temporary file; Write to destination S3.
    md5s = []
    with open(filename, 'rb') as fin:
      with open(s3_uri, 'wb', transport_params={'session': self.s3_session,  'buffer_size': self.chunk_size, 'resource_kwargs': { 'endpoint_url': self.s3_endpoint}, 'multipart_upload': multipart}, ignore_ext=True) as s3_destination:
        for chunk in self.read_in_chunks(fin):
          md5s.append(hashlib.md5(chunk)) #So we can validate the upload
          s3_destination.write(chunk)
           
    #Check ETag generated is the same as the object in the store.
    calculated_etag = self.etag(md5s)
    head = self.s3_connection.head_object(Bucket=bucket, Key=key)
    if calculated_etag != head['ETag']:
      raise Exception( "s3copyfile({}): Etags didn't match".format(filename) )
      
  def s3remove(bucket, key, prefix = None):
    if prefix is not None:
      key = '{}/{}'.format(prefix, key)
    self.s3_connection.delete_object(Bucket = bucket, Key = key)

  def cat_obj_mytardis(self, key, bucket, prefix = None):
    #Read from object and print
    object_name = os.path.basename(key) if prefix is None else '{}/{}'.format(prefix, os.path.basename(key))
    source_s3_uri = 's3://{}/{}'.format(bucket, object_name)
    multipart = False

    with open(source_s3_uri, 'r', transport_params={'session': self.s3_session,  'buffer_size': self.chunk_size, 'resource_kwargs': { 'endpoint_url': self.s3_endpoint}}, ignore_ext=True) as s3_source:
      for chunk in self.read_in_chunks(s3_source):
        print(chunk)

  def bucket_ls(self, bucket, prefix="", suffix=""):
    '''
    Generate objects in an S3 bucket. Derived from AlexWLChan 2019
    :param s3: authenticated client session.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with this prefix (optional).
    :param suffix: Only fetch objects whose keys end with this suffix (optional).
    '''
    paginator = self.s3_connection.get_paginator("list_objects") # should be ("list_objects_v2"), but only getting first page with this

    kwargs = {'Bucket': bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
      prefixes = (prefix, )
    else:
      prefixes = prefix

    for key_prefix in prefixes:
      kwargs["Prefix"] = key_prefix

      for page in paginator.paginate(**kwargs):
        try:
          contents = page["Contents"]
        except KeyError:
          break

        for obj in contents:
          key = obj["Key"]
          if key.endswith(suffix):
            yield obj

  def s3ls(self, bucket, prefix ):
    for r in self.bucket_ls(bucket = bucket, prefix = '{}/'.format(prefix)):
      print(r['Key'], ' ', r['Size'], ' ', r['LastModified'], ' ', r['ETag'])

def json_load(filename):
  try:
    with open( filename ) as f:
      return json.load(f)
  except Exception as e:
    print( "json_load({}): ".format(filename), e )
    sys.exit(1)

def main():
  auth = json_load(os.path.dirname(os.path.realpath(__file__)) + '/conf/rbur004_auth.json')

  s3rsync = S3RSync(s3_keys=auth['src_s3_keys'], s3_endpoint=auth['src_endpoint'])

  filename = os.path.dirname(os.path.realpath(__file__)) + '/data/GeomapKarekare.tif'
  s3rsync.s3copyfile(filename = filename, bucket = auth['bucket'], prefix='Write_Test', key = os.path.basename(filename) )
  s3rsync.s3ls(bucket = auth['bucket'], prefix='Write_Test' )
  
if __name__ == "__main__":
  main()
