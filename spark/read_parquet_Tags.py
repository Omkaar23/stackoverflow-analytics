import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.functions import split, col, explode
from HTMLParser import HTMLParser
from pyspark.sql.types import *

# initialise sparkContext
spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext

# using SQLContext to read parquet file

sqlContext = SQLContext(sc)
 
h = HTMLParser()


"""spark script to read the posts .xml files of different stack exchange communities and convert them to parquet. """
def read_tags_raw(tags_string): # converts <tag1><tag2> to ['tag1', 'tag2']
    return h.unescape(tags_string).strip('>').strip('<').split('><') if tags_string else None



read_tags = udf(read_tags_raw, ArrayType(StringType()))

# to read parquet file
df = spark.read.parquet('s3a://stack-overflow-parquet-files/Posts.parquet/').select('_Id','_ParentId','_Score','_PostTypeId', h.unescape('_Title'), h.unescape('_Body'), read_tags('_Tags').alias('_Tags'), '_AcceptedAnswerId','_AnswerCount','_OwnerUserId','_ViewCount')
df.show()

df.createOrReplaceTempView("pF")

df1 = df.select('_Id', explode('_Tags').alias('_Tag'), '_Score')

df1.show()
df1.createOrReplaceTempView("pF1")

df2 = spark.read.parquet('s3a://stack-overflow-parquet-files/Tags.parquet/').select('_Id', '_TagName', '_Count').orderBy('_Count', ascending = False).limit(100)
df2.show(df2.count(), False)
df2.createOrReplaceTempView("pF2")

df3 = spark.sql('SELECT * FROM pF1 WHERE pF1._Tag IN (SELECT pF2._TagName FROM pF2)')
df3.show()
df3.createOrReplaceTempView('pF3')
df_Tags_avg = spark.sql("SELECT pF3._Tag, CEIL(AVG(pF3._Score)) as _AvgScore from pF3 Group By pF3._Tag Order By _AvgScore DESC")
df_Tags_avg.show()
df_Tags_avg.createOrReplaceTempView('pFT')


df4 = df.join(df3.select('_Id', '_Tag'), '_Id', 'outer').select('_Id','_ParentId','_Score','_PostTypeId', h.unescape('_Title'), h.unescape('_Body'), '_Tag', '_AcceptedAnswerId','_AnswerCount','_OwnerUserId','_ViewCount')
df4.drop('_Tags')
df4.show()

### write parquet files and upload directly to s3
#df4.write.mode('append').parquet("s3a://stack-overflow-parquet-processed/ProcessedTags_Posts.parquet")

df4.createOrReplaceTempView('pF4')
df5 = spark.sql("""SELECT pF4._Id, pF4._ParentId, pF4._Score, pF4._ViewCount, pF4._PostTypeId, pF4._OwnerUserId  FROM pF4 WHERE pF4._Tag ='scala' and pF4._Score < (SELECT pFT._AvgScore from pFT where pFT._Tag = 'scala') LIMIT 100 """)
df5.show()
