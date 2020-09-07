from pyspark import SparkContext,SparkConf
from pyspark import sql
import pyspark

conf = (
    pyspark.SparkConf()
        .setAppName('app_name') 
        .setMaster('local')  
         .set("spark.driver.memory","15g")
        .set('spark.hadoop.fs.s3a.access.key','AKIAR2QXKEWF6JKWPFGR')  
        .set('spark.hadoop.fs.s3a.secret.key','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')  
        )



sc=SparkContext(conf=conf)
spark=sql.SQLContext(sc)
 
 
df=spark.read.csv("s3a://sundarspk/data.csv")

df.show()
'''
df.createOrReplaceTempView("sundar")

df1=spark.sql("select * from sundar")

df1.show()

#df.write.orc("file:/home/hduser/pigdata/sun3")
'''