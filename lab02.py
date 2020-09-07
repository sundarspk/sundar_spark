from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('exerc.com').getOrCreate()


df=spark.read.csv("file:/home/hduser/install/sfpd.csv")
df.printSchema()

df.show()

print(df.count())