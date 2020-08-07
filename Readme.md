# S3 Example, using smart open 

* write_test.py copies the data/Snark.txt to the S3 store Write_Test/Snark.txt object
* read_test.py reads back the object, and prints it to the screen
* delete_test.py removes the object from the store.

## Conf file conf/test_auth.json

```
{
  "src_endpoint": "https://xxxxxxx.xxxx",
  "src_s3_keys": {
    "access_key_id": "xxxxxxxxxxxxxxx",
    "secret_access_key": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
   },
   "bucket": "bucket_name"
}
```
