
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.io.FileReader
import com.opencsv.CSVReader
import org.apache.spark.sql.types._
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

object Utilities {

  def load_csv(path: String, schema: StructType)(implicit spark: SparkSession): DataFrame = {

    //This reads the csv as a table of Strings and outs as a Java linked list
    //hence has to be converted to Scala Buffer.
    def opencsv_entries(): mutable.Buffer[Array[String]] = {
      val reader = new CSVReader(new FileReader(path))
      val myEntries = reader.readAll()
      myEntries.asScala
    }

    def typefix(array: Array[String], schema: StructType): Row = {
      val rowSeq: Seq[Any] = array.zip(schema.fields).map { case (value, field) =>
        field.dataType match {
          case IntegerType => if (value!="UNKNOWN") value.toInt else 0
          case DoubleType => if (value!="UNKNOWN") value.toDouble else 0.0
          case _ => value
        }
      }
      Row.fromSeq(rowSeq)
    }

    val rdd = spark.sparkContext.parallelize(opencsv_entries())
    val header = rdd.first()
    val data = rdd.filter(row => !(row sameElements header))

    val rowRDD = data.map(array => typefix(array, schema))
    spark.createDataFrame(rowRDD, schema)

  }

}
