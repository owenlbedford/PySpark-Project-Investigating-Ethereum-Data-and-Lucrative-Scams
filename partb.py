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
            int(fields[7])
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
    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    clean_lines=lines.filter(good_line)
    contracts=clean_lines.map(lambda l: (l.split(',')[0], 1))
    print(contracts.take(10))
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines2 = transactions.filter(good_line2)
    transactionsFeatures=clean_lines2.map(lambda l:(l.split(',')[6] ,int(l.split(',')[7])))
    transactionsFeatures=transactionsFeatures.reduceByKey(lambda x,y: x+y)
    print(transactionsFeatures.take(10))
    transactionData=transactionsFeatures.join(contracts)
    print(transactionData.take(10))
    #data=transactionData.map(lambda x:(x[0], x[1][0]))
    #print(data.take(1000))
    #data=data.reduceByKey(operator.add)
    #print(data.take(1000))
    top10=transactionData.takeOrdered(10, key=lambda x: -x[1][0])

    
    
    
    

    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'partb_' + date_time + '/partb.txt')
    my_result_object.put(Body=json.dumps(top10))
    #my_result_object = my_bucket_resource.Object(s3_bucket,'partb_' + date_time + '/partb.txt')
    #my_result_object.put(Body=json.dumps(sectorData.take(1000)))

    spark.stop()
