from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('exerc.com').master("local[*]").getOrCreate()

dept = [("Finance",10), 
        ("Marketing",20), 
        ("Sales",30), 
        ("IT",40) 
      ]     

df=spark.sparkContext.parallelize(dept) 

dept_header=["deptname","count"]

df1=spark.createDataFrame(dept,dept_header)

df1.printSchema()

df1.show()