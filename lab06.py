
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com') \
        .master("local[5]").getOrCreate()

df=spark.range(0,20)
print(df.rdd.getNumPartitions())

df.show()

df.write.mode("overwrite").csv("file:/home/hduser/hive/data/pyspark")

df2 = df.repartition(6)
print(df2.rdd.getNumPartitions())

df2.write.mode("overwrite").csv("file:/home/hduser/hive/data/pyspark_rep")

df3 = df.coalesce(2)
print(df3.rdd.getNumPartitions())

df3.write.mode("overwrite").csv("file:/home/hduser/hive/data/pyspark_coal")