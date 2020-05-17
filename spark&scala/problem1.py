from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F 
from pyspark.sql.types import StringType, FloatType, IntegerType, StructType, StructField
spark=SparkSession.builder.appName("Project3Problem1").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

c_schema =StructType([
        StructField("ID", IntegerType()),
        StructField("Name", StringType()),
        StructField("Age", IntegerType()),
        StructField("CountryCode", StringType()),
	StructField("Salary", FloatType())])

customers = spark.read.csv(path="hdfs://localhost:9000/user/Customers.txt",schema=c_schema)
customers.createOrReplaceTempView("Customers")

p_schema =StructType([
        StructField("TransID", IntegerType()),
        StructField("CustID", IntegerType()),
        StructField("TransTotal", FloatType()),
        StructField("TransNumItems", IntegerType()),
	StructField("TransDesc", StringType())])

purchases = spark.read.csv(path="hdfs://localhost:9000/user/Purchases.txt",schema=p_schema)
purchases.createOrReplaceTempView("Purchases")


T1 = spark.sql("SELECT * FROM Purchases WHERE TransTotal <= 600")
T1.write.json(path="hdfs://localhost:9000/user/T1",mode="overwrite")

T2 = T1.groupby("TransNumItems").agg(F.min("TransTotal"),F.max("TransTotal"),F.expr("percentile_approx(TransTotal,0.5)").alias("median(TransTotal)"))
T2.write.json(path="hdfs://localhost:9000/user/T2",mode="overwrite")

T3 = T1.join(customers,T1.CustID==customers.ID,"inner")
T3 = T3.filter((T3.Age<=25) & (T3.Age>=18)).groupby("CustID")
T3 = T3.agg(F.first("Age").alias("Age"),F.sum("TransNumItems").alias("TotalItems"),F.sum("TransTotal").alias("TotalSpent"))
T3.write.json(path="hdfs://localhost:9000/user/T3",mode="overwrite")

rows = T3.collect()
hits=[]
n=len(rows)
for i in range(n):
	C1=rows[i]
	for j in range(i+1,n):
		C2=rows[j]
		cond1=(C1.Age < C2.Age) & (C1.TotalSpent > C2.TotalSpent) & (C1.TotalItems < C2.TotalItems)
		cond2=(C2.Age < C1.Age) & (C2.TotalSpent > C1.TotalSpent) & (C2.TotalItems < C1.TotalItems)
		if cond1:
			hits.append((C1.CustID,C2.CustID,C1.TotalSpent,C2.TotalSpent,C1.TotalItems,C2.TotalItems))
		if cond2:
			hits.append((C2.CustID,C1.CustID,C2.TotalSpent,C1.TotalSpent,C2.TotalItems,C1.TotalItems))

T4 = spark.createDataFrame(hits,["C1","C2","TotalAmount1","TotalAmount2","TotalItemCount1","TotalItemCount2"])
T4 = T4.join(customers,T4.C1 == customers.ID)
T4 = T4.selectExpr("C1","C2","Age as Age1","TotalAmount1","TotalAmount2","TotalItemCount1","TotalItemCount2")
T4 = T4.join(customers,T4.C2 == customers.ID)
T4 = T4.selectExpr("C1 as C1_ID","C2 as C2_ID","Age1","Age as Age2","TotalAmount1","TotalAmount2","TotalItemCount1","TotalItemCount2")
T4.write.json(path="hdfs://localhost:9000/user/T4",mode="overwrite")

spark.stop()
#To run on the terminal:
#~/spark-2.3.2-bin-hadoop2.7/bin/spark-submit problem1.py
