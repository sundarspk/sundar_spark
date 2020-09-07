from pyspark import SparkContext,SparkConf
from pyspark import sql
import pyspark

conf = (
    pyspark.SparkConf()
        .setAppName('app_name') 
        .setMaster('local')  
        #.set('spark.hadoop.fs.s3a.access.key','AKIAJ3KQ7XPKWGY3F67Q')  
        #.set('spark.hadoop.fs.s3a.secret.key','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')  
        .set('spark.hadoop.fs.s3a.access.key','AKIAR2QXKEWFX6KX3D65')  
        .set('spark.hadoop.fs.s3a.secret.key','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')  
        )



sc=SparkContext(conf=conf)
spark=sql.SQLContext(sc)
 
 
df=spark.read.csv('s3a://sundarspk/')

df.createOrReplaceTempView("sundar")

df1=spark.sql("select * from sundar")

df1.show(10)