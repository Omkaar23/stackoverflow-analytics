#!/usr/bin/python

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


def main():
	conf = SparkConf().set(\
	    "spark.jars",\
	    "/usr/local/spark/jars/postgresql-42.2.9.jar,")


	sc = SparkContext(conf=conf)

	spark = SparkSession.builder.getOrCreate()

	df7 = spark.read.parquet('s3a://stack-overflow-parquet-processed/df7.parquet/')

	df7.write.format('jdbc') \
	        .option("url", " jdbc:postgresql://'HOST_URL':'PORT'/'DB_NAME' ") \
	    	.option("dbtable", "TABLE_NAME") \
	    	.option("user", "USER") \
	    	.option("password", "PASSWORD") \
	   		.option("driver", "org.postgresql.Driver") \
	    	.mode("append").save()


if __name__ == "__main__":
   main()
