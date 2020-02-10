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


conf = SparkConf().set(\
    "spark.jars",\
    "/usr/local/spark/jars/hadoop-aws-2.7.3.jar")


sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

# df3 contains only the Posts form the top 100 tags ['_PostId', '_PostTag', '_Score']
df3 = spark.read.parquet('s3a://stack-overflow-parquet-processed/df3.parquet/')
df3.show()
df3.createOrReplaceTempView('pF3')

df_Tags_avg = spark.sql("SELECT pF3._PostTag, CEIL(AVG(pF3._Score)) as _AvgScore from pF3 Group By pF3._PostTag Order By _AvgScore DESC")
df_Tags_avg.show()
df_Tags_avg.createOrReplaceTempView('pFT')


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
