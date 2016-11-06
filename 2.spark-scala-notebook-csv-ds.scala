val lines=sc.textFile("/mg-programming/96.BIGDATA/0.Workspace/Cartier+for+WinnersCurse.csv")
val header = lines.first
//header:	String	=	auctionid,bid,bidtime,bidder,bidderrate,openbid,price
case class Auction(auctionid:String , bid:Float , bidtime:Float , bidder:String,bidderrate:Int,openbid:Float,price:Float)
val dataRecords = lines.filter(_ != header)
val dataRecordAuctions = dataRecords.map(_.split(",")).map(row => Auction(row(0),row(1).toFloat,row(2).toFloat,row(3),row(4).toInt,row(5).toFloat,row(6).toFloat))
val dataRecordAuctionsDF = dataRecordAuctions.toDF 

//read csv
//install dependency
./bin/spark-shell --packages com.databricks:spark-csv_2.11:1.2.0
val csvAuctionsDF = spark.read.format("com.databricks.spark.csv").option("header",true).load("/mg-programming/96.BIGDATA/0.Workspace/Cartier+for+WinnersCurse.csv")
csvAuctionsDF.printSchema
csvAuctionsDF.show
//group , distinct and counting operations
csvAuctionsDF.groupBy("bidder").count().show
csvAuctionsDF.groupBy('bidder).count().show
csvAuctionsDF.groupBy('bidder).count().sort($"count".desc).show
csvAuctionsDF.groupBy('bidder).count().sort(desc("count")).show
csvAuctionsDF.select("auctionid").distinct.count
csvAuctionsDF.groupBy("bidder").count.show

//SQL Functions
csvAuctionsDF.registerTempTable("auctions")
val sql = spark.sql("SELECT COUNT(*) AS COUNT FROM AUCTIONS")
sql.explain
sql.show
val count=sql.collect()(0).getLong(0)

case class Token(name:String , productId:Int , score:Double)
val data = Seq(Token("aaa",100,0.12) , Token("aaa",200,0.29) , Token("bbb",200,0.53) , Token("bbb",300,0.64))
val dataDS = data.toDS
val dataDF = data.toDF
dataDF.filter($"name" like ("a%")).show

//Apache AVRO
./spark-shell --packages com.databricks:spark-avro_2.11:2.0.0 --repositories "http://dl.bintray.com/databricks/maven"

val fileRdd	=sc.textFile("README.md")
val df=fileRdd.toDF
import org.apache.spark.sql.SaveMode
val outputF="test.avro"
df.write.mode(SaveMode.Append).format("com.databricks.spark.avro").save(outputF)



