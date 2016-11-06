import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.{IntegerType,StringType,LongType}
import org.apache.spark.sql.types.DataTypes

val schemaUntyped= new StructType().add("a","int").add("b","string")
val schemaTyped=new StructType().add("a",IntegerType).add("b",StringType)
val schemaWithMap = StructType(StructField("map",createMapType(LongType,StringType),false) :: Nil)
schemaWithMap.printTreeString
println(schemaWithMap.prettyJson)

schemaTyped.sql
schemaTyped.simpleString
schemaTyped.catalogString

//add an existing struct to a schemaTyped
schemaTyped(names=Set("a"))
schemaTyped("a").getComment

//DataTypes Factories
val arrayType = DataTypes.createArrayType(BooleanType)
val mapType = DataTypes.createMapType(LongType,StringType)

import org.apache.spark.sql.Encoders
