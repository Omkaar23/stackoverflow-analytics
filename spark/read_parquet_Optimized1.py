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
    "/usr/local/spark/jars/postgresql-42.2.9.jar,")


sc = SparkContext(conf=conf)


# initialise sparkContext
spark = SparkSession.builder.getOrCreate()

#sc = spark.sparkContext

# using SQLContext to read parquet file

sqlContext = SQLContext(sc)
 
h = HTMLParser()


"""spark script to read the posts .xml files of different stack exchange communities and convert them to parquet. """
def read_tags_raw(tags_string): # converts <tag1><tag2> to ['tag1', 'tag2']
    return h.unescape(tags_string).strip('>').strip('<').split('><') if tags_string else None



read_tags = udf(read_tags_raw, ArrayType(StringType()))

# to read parquet file
df = spark.read.parquet('s3a://stack-overflow-parquet-files/Posts.parquet/').select('_Id','_ParentId','_Score','_PostTypeId', h.unescape('_Title'), h.unescape('_Body'), read_tags('_Tags').alias('_Tags'), '_AcceptedAnswerId','_AnswerCount','_OwnerUserId')
df.show()
df.createOrReplaceTempView("pF")

df1 = df.select(col('_Id').alias('_PostId'), explode('_Tags').alias('_PostTag'), '_Score')
#df1.withColumnRenamed('_Id', '_PostId').collect()
df1.show()
df1.createOrReplaceTempView("pF1")

df2 = spark.read.parquet('s3a://stack-overflow-parquet-files/Tags.parquet/').select('_Id', '_TagName', '_Count').orderBy('_Count', ascending = False).limit(100)
df2.show(df2.count(), False)
df2.createOrReplaceTempView("pF2")

df3 = spark.sql('SELECT * FROM pF1 WHERE pF1._PostTag IN (SELECT pF2._TagName FROM pF2)')
df3.show()
df3.createOrReplaceTempView('pF3')
#df_Tags_avg = spark.sql("SELECT pF3._PostTag, CEIL(AVG(pF3._Score)) as _AvgScore from pF3 Group By pF3._PostTag Order By _AvgScore DESC")
#df_Tags_avg.show()
#df_Tags_avg.createOrReplaceTempView('pFT')


#df4 = df.join(df3.select('_PostId','_PostTag'), (df._Id == df3._PostId), 'left')
#df4.show()
#df4.createOrReplaceTempView('pF4')

df_Tag_Id = spark.sql(""" SELECT pF3._PostId FROM pF3 WHERE pF3._PostTag = 'scala' """)
df_Tag_Id.show()
df_Tag_Id.createOrReplaceTempView('pFTagId')

df5 = spark.sql("""SELECT pF._Id, pF._ParentId, pF._Score, pF._PostTypeId, pF._Title, pF._Body,pF._Tags, pF._AcceptedAnswerId, pF._AnswerCount, pF._OwnerUserId from pF WHERE pF._Id IN (SELECT pFTagId._PostId from PfTagId) or (pF._ParentId IS NOT NULL and pF._ParentId IN (SELECT pFTagId._PostId from PfTagId)) """) 
df5.show()
#df5 = df4.select('_Id','_ParentId','_Score','_PostTypeId', '_Title', '_Body', '_PostTag', '_AcceptedAnswerId','_AnswerCount','_OwnerUserId')
#df4.drop('_Tags')