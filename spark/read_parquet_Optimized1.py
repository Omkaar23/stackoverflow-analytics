import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.functions import split, col, explode
from HTMLParser import HTMLParser
from pyspark.sql.types import *



""" spark script to read the posts .xml files of different stack exchange communities and convert them to parquet """
def read_tags_raw(tags_string): # converts <tag1><tag2> to ['tag1', 'tag2']
    return h.unescape(tags_string).strip('>').strip('<').split('><') if tags_string else None


def main():

	conf = SparkConf().set(\
    "spark.jars",\
    "/usr/local/spark/jars/hadoop-aws-2.7.3.jar")

	sc = SparkContext(conf=conf)
	# initialise sparkContext
	spark = SparkSession.builder.getOrCreate()
	h = HTMLParser()

	
	read_tags = udf(read_tags_raw, ArrayType(StringType()))

	# to read parquet file
	df = spark.read.parquet('s3a://stack-overflow-parquet-files/Posts.parquet/').select('_Id','_ParentId','_Score','_PostTypeId', h.unescape('_Title'), h.unescape('_Body'), read_tags('_Tags').alias('_Tags'), '_AcceptedAnswerId','_AnswerCount','_OwnerUserId')
	df.show()
	df.createOrReplaceTempView("pF")

	df1 = df.select(col('_Id').alias('_PostId'), explode('_Tags').alias('_PostTag'), '_Score')
	df1.show()
	df1.createOrReplaceTempView("pF1")

	df2 = spark.read.parquet('s3a://stack-overflow-parquet-files/Tags.parquet/').select('_Id', '_TagName', '_Count').orderBy('_Count', ascending = False).limit(100)
	df2.show(df2.count(), False)
	df2.createOrReplaceTempView("pF2")

	# df3 contains only the Posts form the top 100 tags ['_PostId', '_PostTag', '_Score']
	df3 = spark.read.parquet('s3a://stack-overflow-parquet-processed/df3.parquet/')
	df3.show()
	df3.createOrReplaceTempView('pF3')

	df_Tags_avg = spark.sql("SELECT pF3._PostTag, CEIL(AVG(pF3._Score)) as _AvgScore from pF3 Group By pF3._PostTag Order By _AvgScore DESC")
	df_Tags_avg.show()
	df_Tags_avg.createOrReplaceTempView('pFT')
	df_Tags_avg.write.mode('append').parquet('s3a://stack-overflow-parquet-processed/df_Tags_avg.parquet/')


	# df4 contains a join of Posts and Exploded Tags ['_Id','_ParentId','_Score','_PostTypeId','_OwnerUserId','_PostId','_PostTag']
	df4 = spark.read.parquet('s3a://stack-overflow-parquet-processed/df4.parquet/')
	df4.createOrReplaceTempView('pF4')
	df4.show(1000)


	df5 = spark.sql("""SELECT pF4._Id, pF4._ParentId, pF4._Score, pF4._PostTypeId, pF4._PostTag, pF4._OwnerUserId \
	 					FROM pF4 \
	 					WHERE (pF4._Id IN (SELECT pF3._PostId FROM pF3 WHERE pF3._PostTag = 'scala') OR pF4._ParentId IN (SELECT pF3._PostId FROM pF3 WHERE pF3._PostTag = 'scala')) and \
	 						   pF4._Score < (SELECT pFT._AvgScore from pFT where pFT._PostTag = 'scala') \
	 					 """)
	df5.show(1000)
	df5.createOrReplaceTempView('pF5')

	# df6 contains columns from Users table ['_UserId', '_Reputation', '_Views']
	df6 = spark.read.parquet('s3a://stack-overflow-parquet-files/Users.parquet/').select(col('_Id').alias('_UserId'), '_Reputation')
	df6.show()
	df6.createOrReplaceTempView('pF6')


	# df7 contins a join of Posts, Tags, Users ['_Id','_ParentId','_Score','_PostTypeId','_OwnerUserId','_PostId','_PostTag', '_Reputation']
	#df7 = df4.join(df6.select('_Reputation').alias('_UserReputation'), (df4._OwnerUserId == df6._UserId), 'left')
	#df7.show()



	df7 = spark.sql(""" SELECT pF4._Id, pF4._ParentId, pF4._Score, pF4._PostTypeId, pF4._PostTag, pF4._OwnerUserId, pF6._Reputation as _UserReputation \
						FROM pF4, pF6 \
						WHERE pF4._OwnerUserId = pF6._UserId """)
	df7.show(1000)

	df7.write.mode('append').parquet('s3a://stack-overflow-parquet-processed/df7.parquet/')



if __name__ == "__main__":
   main()
