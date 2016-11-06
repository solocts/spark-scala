///mg-programming/96.BIGDATA/0.Workspace/Cartier+for+WinnersCurse.csv

import org.apache.spark.sql.types.{StringType,StructType}
import org.apache.spark.sql.Row


val lines=sc.textFile("/mg-programming/96.BIGDATA/0.Workspace/Cartier+for+WinnersCurse.csv")
//get the headers
val headers=lines.first
//create the schema from it
val auctionFields=headers.split(",").map(f => StructType(f,StringType))
val auctionSchema=StructField(auctionFields)
//filter the headers
val noheaders = lines.filter(_ != headers)
//for each record , create a RDD row
val rows = noheaders.map(_=>_.split(",")).map(col => Row.fromSeq(col))
//create a dataframe
val auctionDF = spark.createDataFrame(rows,auctionSchema)
//show the data
auctionDF.show