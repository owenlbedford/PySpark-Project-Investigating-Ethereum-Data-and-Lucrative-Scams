"""Lab 4. Joining Datasets using NASDAQ data
"""
import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("NASDAQ")\
        .getOrCreate()

    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=6:
                return False
            return True
        except:
            return False
        
    def good_line2(line):
        try:
            fields = line.split(',')
            if len(fields) != 15:
                return False
            int(fields[8])
            int(fields[7])
            return True
        except:
            return False
        
    def filterByTop10(rdd):
        A=["0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444", "0x7727e5113d1d161373623e5f49fd568b4f543a9e", "0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef", "0xbfc39b6f805a9e40e77291aff27aee3c96915bdd", "0xe94b04a0fed112f3664e45adb2b8915693dd5ff3", "0xabbb6bebfa05aa13e908eaa492bd7a8343760477", "0x341e790174e3a4d35b65fdc067b6b5634a61caea", "0x58ae42a38d6b33a1e31492b60465fa80da595755", "0xc7c7f6660102e9a1fee1390df5c76ea5a5572ed3", "0xe28e72fcf78647adce1f1252f240bbfaebd63bcc"]
        for i in range(len(A)):
            if rdd[0] == A[i]:
                return True
        return False
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    
    transactions2 = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines3 = transactions2.filter(good_line2)
    transactionsFeatures2=clean_lines3.map(lambda l: (l.split(',')[6], (time.strftime("%Y - %m", time.gmtime(int(l.split(',')[11]))), int(l.split(',')[8]))))
    transactionsFeatures2 = transactionsFeatures2.filter(filterByTop10)
    print(transactionsFeatures2.take(1000))
    transactionsFeatures2 = transactionsFeatures2.map(lambda l: (l[1][0], l[1][1]))
    print(transactionsFeatures2.take(1000))
    valuesRecords=spark.sparkContext.broadcast(transactionsFeatures2.countByKey())
    transactionsFeatures2=transactionsFeatures2.reduceByKey(lambda x,y: x+y)
    transactionsFeatures2=transactionsFeatures2.map(lambda x: (x[0], x[1]/valuesRecords.value[x[0]]))



    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'partdmisc1c' + date_time + '/partd_c_gasvaluesfortop10.txt')
    my_result_object.put(Body=json.dumps(transactionsFeatures2.take(300)))
    

    spark.stop()
