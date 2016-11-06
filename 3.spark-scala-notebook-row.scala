//Row is a collection of fields
import org.apache.spark.sql.Row
val row = Row(1,"hello")
row(1)
row.get(1)

Row.fromSeq(Seq(1 ,"world"))
Row.fromTuple((1,"world"))
Row.merge(Row(1),Row("hello"))
Row.empty==Row()

Row.unapplySeq(Row(1,"hello"))
Row(1,"hello") match {
    case Row(key:Int , value:String) => key->value
}