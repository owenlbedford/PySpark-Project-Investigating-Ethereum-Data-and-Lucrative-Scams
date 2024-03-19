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
    
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines2 = transactions.filter(good_line2)
    transactionsFeatures2=clean_lines2.map(lambda l: (l.split(',')[6], (time.strftime("%Y - %m", time.gmtime(int(l.split(',')[11]))), int(l.split(',')[7]))))
    
    #fix csv missing top row
    #Join with transaction (using address (to address))
    #Make Id Key, aggregate by value
    #Highest value is the most lucrative scam
    #Most popular scams (top 10) how they change over time graph full 10 marks
    #Most lucrative scam is (5 marks)
    
    
    #Also do with most popular for extra 5 hhkkkhjjjkkh
    
    
    scamsRdd = spark.sparkContext.textFile("s3a://" + s3_bucket + "/scamsfixed.csv")
    scamsRdd2 = scamsRdd.map(lambda x: (x.split(',')[1][1:], x.split(',')[0]))
    print(scamsRdd2.take(5))
    #scamsRdd2 = scamsRdd2.reduceByKey(lambda x,y: x+y) j_said
    #(address, id)
    #(address, (time, value))
    #(address, ((time, value), id))
    transactionScams = transactionsFeatures2.join(scamsRdd2)
    transactionScams = transactionScams.map(lambda x: (x[1][1], x[1][0][1]))
    transactionScams = transactionScams.reduceByKey(lambda x,y: x+y)
    top10Scams = transactionScams.takeOrdered(10, key=lambda x: -x[1])
    #(address, (time, ether))
    #(address, 1)
    #(address, ((time, ether), 1)))
    #(time, ether)
    valuesRecords2=spark.sparkContext.broadcast(transactionScams.countByKey())
    transactionScamsReduced = transactionScams.reduceByKey(lambda x,y: x+y)
    transactionScamsReduced = transactionScamsReduced.map(lambda x: (x[0], x[1]/valuesRecords2.value[x[0]]))

    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'scamstop10' + date_time + '/partd_scamstop10.txt')
    my_result_object.put(Body=json.dumps(top10Scams))
    

    spark.stop()
