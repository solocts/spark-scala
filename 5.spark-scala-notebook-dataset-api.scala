//as     -  Converting	a Dataset to a Dataset

//explain - Explain execution of the query
dataSetShow.explain
//filter -  Filter the data
dataSetShow.filter(_.contains("Hello")).show
//flatMap - Flatten a list
val dataSetSample = Seq(("Hello","world"),("Hola","Hola")).toDS
dataSetSample.flatMap(t => List(t._1,t._2)).show
//foreachPartition
//isLocal - if the partition is local or in a cluster
dataSetSample.isLocal
//isStreaming - if it is read through a stream
dataSetSample.isStreaming
//mapPartition
//randomSplit
//rdd
//repartition
//schema
//select
//selectExpr
//show 
val dataSetShow = Seq("Hello","world").toDS
dataSetShow.show
//take -    Take a sample value
dataSetShow.take(1)
//toDF -   Converts a Dataset to a DataFrame	
//toJSON - Converts a dataset to a JSON representation
dataSetShow.toJSON
//transform : Transforms a Dataset
import org.apache.spark.sql.Dataset
def withDoubled(longs:Dataset[java.lang.Long])=longs.withColumn("doubled" , 'id * 2)
val dataset = spark.range(5)
dataset.transform(withDoubled).show
//write
//writeStream