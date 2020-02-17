#!/usr/bin/python

from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
#import boto3

# add jars
conf = SparkConf().set(\
    "spark.jars",\
    "/usr/local/spark/jars/spark-xml_2.11-0.8.0.jar,")

sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

users_file = "s3a://stack-overflow-file-dump-xml/stackoverflow.com-Tags/Tags.xml"
#comments_file = "s3a://stack-overflow-file-dump-xml/stackoverflow.com-Comments/Comments.xml"
#posts_file = "s3a://stack-overflow-datadump-xml/stackoverflow.com-Posts/Posts.xml"


### load xml files from s3 bucket
df_users = spark.read \
    .format('xml') \
    .options(rowTag='row') \
    .load(users_file)

### write parquet files and upload directly to s3
df_users.write.parquet("s3a://stack-overflow-parquet-files/Tags.parquet")
#df_comments.write.mode('append').partitionBy("UserId").parquet("s3a://stack-overflow-parquet-files/Comments.parquet")

