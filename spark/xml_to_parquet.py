#!/usr/bin/python

from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import boto3



def main():
	# add jars
	conf = SparkConf().set(\
	    "spark.jars",\
	    "/usr/local/spark/jars/spark-xml_2.11-0.8.0.jar,")

	sc = SparkContext(conf=conf)

	spark = SparkSession.builder.getOrCreate()

	s3 = boto3.resource('s3')
	my_bucket = s3.Bucket('stack-overflow-file-dump-xml/')
	new_bucket = s3.Bucket('stack-overflow-parquet-files/')
	for files in my_bucket.objects.all():
	    # load xml files from s3 bucket
	    df = spark.read.format('xml').options(rowTag='row').load("s3a://"+ files.bucket_name + files.key)
		
		# write parquet files and upload directly to s3
		df.write.mode('append').parquet.("s3a://" + new_bucket + files.key.split('/',1)[1])

if __name__ == "__main__":
   main()
