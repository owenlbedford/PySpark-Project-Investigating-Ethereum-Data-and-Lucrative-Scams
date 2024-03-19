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
            int(fields[9])
            return True
        except:
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
    #spark.sparkContext.setLogLevel('WARN')
    
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines2 = transactions.filter(good_line2)
    transactionsFeatures=clean_lines2.map(lambda l: (time.strftime("%Y - %m", time.gmtime(int(l.split(',')[11]))),int(l.split(',')[9])))
    valuesRecords=spark.sparkContext.broadcast(transactionsFeatures.countByKey())
    transactionsFeatures = transactionsFeatures.reduceByKey(lambda x,y: x+y)
    transactionsFeatures = transactionsFeatures.map(lambda x:(x[0],x[1]/valuesRecords.value[x[0]]))
    print(transactionsFeatures.take(100))
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'partdmisc1a' + date_time + '/partdmisc1a.txt')
    my_result_object.put(Body=json.dumps(transactionsFeatures.take(10000000000)))

    spark.stop()
