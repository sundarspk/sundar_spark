import pyspark
from pyspark.sql import SparkSession
#from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.printSchema()
df.show()


#df3 = df.withColumn("salary",col("salary")*100)
#df3.printSchema()
##df3.show(truncate=False)

#df5 = df.withColumn("Country", lit("USA"))
#df5.printSchema()
#df5.show()

df.withColumnRenamed("gender","sex") \
  .show(truncate=False) 