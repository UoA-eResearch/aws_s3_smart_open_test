#!/usr/bin/python3

import os
import sys
import boto3
import json

def json_load(filename):
  try:
    with open( filename ) as f:
      return json.load(f)
  except Exception as e:
    print( "json_load({}): ".format(filename), e )
    sys.exit(1)

def main():
  auth = json_load(os.path.dirname(os.path.realpath(__file__)) + '/conf/test_auth.json')
  
  src_session = boto3.Session(
       aws_access_key_id=auth['src_s3_keys']['access_key_id'],
       aws_secret_access_key=auth['src_s3_keys']['secret_access_key']
  )

  src_connection = src_session.client(
      's3',
      aws_session_token=None,
      region_name='us-east-1',
      use_ssl=True,
      endpoint_url=auth['src_endpoint'],
      config=None
  )

  src_connection.delete_object(Bucket= auth['bucket'], Key='Write_Test/Snark.txt')

if __name__ == "__main__":
  main()
