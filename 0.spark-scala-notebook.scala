import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

val scoreSchema=StructType(StructField("id",LongType,nullable=false) :: 
                           StructField("name",StringType,nullable=false) :: 
                           StructField("score",LongType ,nullable=false)::Nil)

spark.
readStream.
format("json").
load("/mg-programming/96.BIGDATA/0.Workspace/input-records.json").
schema(scoreSchema).where('score > 42).
writeStream.format("console")

spark.
read.
format("json").load("/mg-programming/96.BIGDATA/0.Workspace/input-records.json").
select("id","name","score").
where('score > 44).
write.
format("csv")
.save("/mg-programming/96.BIGDATA/0.Workspace/output-records.csv")


//spark session and basics of querying
import org.apache.spark.sql.SparkSession
val sparkSession:SparkSession = SparkSession.builder.master("local[*]").appName("Spark Session Example").getOrCreate

// variants in dataset query
val dataset = (0 to 4).toDS
//	Variant	1:	filter	operator	accepts	a	Scala	function
dataset.filter(n	=>	n	%	2	==	0).count
//	Variant	2:	filter	operator	accepts	a	Column-based	SQL	expression
dataset.filter('value	%	2	===	0).count
//	Variant	3:	filter	operator	accepts	a	SQL	query
dataset.filter("value	%	2	=	0").count

//dataset and dataframe
val ds = Seq("I am a dataset").toDS
val df = Seq("I am a dataset").toDF
val df = Seq("I am a dataset").toDF("text")
val pDs = sc.parallelize(Seq("hello")).toDS


//implicit imports and version from the spark-shell
spark.version and :imports

//serialize and deserialize
case class Token(name:String , productId:Int , score:Double)
val data = Seq(Token("aaa",100,0.12) , Token("aaa",200,0.29) , Token("bbb",200,0.53) , Token("bbb",300,0.64))
val dataDS = data.toDS
val dataDF = data.toDF
//convert from DataFrame to DataSet
dataDF.as[Token].show

//find out the class names for each of the rows
dataDS.map(_.getClass.getName).show(false)
dataDF.map(_.getClass.getName).show(false)

case class Person(id:Long , name:String)

import org.apache.spark.sql.Encoders
val personEncoder = Encoders.product[Person]
personEncoder.schema
personEncoder.clsTag

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
val personExprEncoder=personEncoder.asInstanceOf[ExpressionEncoder[Person]]
personExprEncoder.serializer
val ram = Person(0,"Ram")
personExprEncoder.toRow(ram)


//DataFrames
val numberDF = Seq(("one",1),("one",1),("one",1),("two",1)).toDF("word","count")
numberDF.show
numberDF.groupBy('word).count.show

//Dataframe to Dataset for a specific schema

final case class MyRecord(id:Int , token:String)
val df = Seq("hello","world").zipWithIndex.map(_.swap).toDF("id","token")
val ds = df.as[(Int,String])
val ds2 = df.as[MyRecord)

//Dataframe from scratch
case class Person(name:String , age:Int)
val persons = Seq(Person("Ram",24),Person("Shyam",20),Person("Raghu",30))
val personDF = spark.createDataFrame(persons)
personDF.show