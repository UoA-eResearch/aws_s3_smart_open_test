#!/usr/bin/env python3
import os
import sys
import hashlib
import codecs

CHUNK_SIZE = 1048576 #1M

def read_in_chunks(file_object, chunk_size):
  '''
    Iterator to read a file chunk by chunk.
  
    file_object: file opened by caller
  '''
  while True:
    data = file_object.read(chunk_size)
    if not data:
      break
    yield data

def etag(md5_array):
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


filename = os.path.dirname(os.path.realpath(__file__)) + '/data/GeomapKarekare.tif'
md5s = []
with open(filename, 'rb') as fin:
  for chunk in read_in_chunks(fin, CHUNK_SIZE):
    md5s.append(hashlib.md5(chunk)) #So we can validate the upload
  
calculated_etag = etag(md5s)

print( calculated_etag )
