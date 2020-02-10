from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
# initialise sparkContext

conf = SparkConf().set(\
    "spark.jars",\
    "/usr/local/spark/jars/postgresql-42.2.9.jar,")


sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

# using SQLContext to read parquet file

#sqlContext = SQLContext(sc)

# to read parquet file

# df3 contains only the Posts form the top 100 tags ['_PostId', '_PostTag', '_Score']
df3 = spark.read.parquet('s3a://stack-overflow-parquet-processed/df3.parquet/')
df3.show()
df3.createOrReplaceTempView('pF3')

df_Tags_avg = spark.sql("SELECT pF3._PostTag, CEIL(AVG(pF3._Score)) as _AvgScore from pF3 Group By pF3._PostTag Order By _AvgScore DESC")
df_Tags_avg.show()
df_Tags_avg.createOrReplaceTempView('pFT')

df2 = spark.sql(""" SELECT COUNT(*) FROM pFT """)
df2.show()

df_Tags_avg.write.format('jdbc') \
        .option("url", "jdbc:postgresql://ec2-3-92-169-255.compute-1.amazonaws.com:5432/postgres") \
    .option("dbtable", "public.df_Tags_Avg") \
    .option("user", "postgres") \
    .option("password", "") \
   .option("driver", "org.postgresql.Driver") \
    .mode("append").save()
