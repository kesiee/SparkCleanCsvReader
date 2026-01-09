import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import com.opencsv.CSVReader
import java.io.FileReader
import scala.collection.JavaConverters._
import scala.util.{Try, Using}

object Utilities {

  def loadCsv(path: String, schema: StructType)(implicit spark: SparkSession): DataFrame = {
    val entries = readCsvFile(path)
    val dataWithoutHeader = entries.drop(1) // More efficient than filter comparison
    
    val rowRDD = spark.sparkContext
      .parallelize(dataWithoutHeader)
      .map(array => parseRow(array, schema))
    
    spark.createDataFrame(rowRDD, schema)
  }

  private def readCsvFile(path: String): List[Array[String]] = {
    Using.resource(new CSVReader(new FileReader(path))) { reader =>
      reader.readAll().asScala.toList
    }
  }

  private def parseRow(values: Array[String], schema: StructType): Row = {
    val parsed = values.zip(schema.fields).map { case (value, field) =>
      parseValue(value.trim, field.dataType)
    }
    Row.fromSeq(parsed)
  }

  private def parseValue(value: String, dataType: DataType): Any = {
    if (value.isEmpty || value.equalsIgnoreCase("UNKNOWN")) {
      getNullValue(dataType)
    } else {
      dataType match {
        case IntegerType  => value.toInt
        case LongType     => value.toLong
        case DoubleType   => value.toDouble
        case FloatType    => value.toFloat
        case BooleanType  => value.toBoolean
        case _            => value
      }
    }
  }

  private def getNullValue(dataType: DataType): Any = dataType match {
    case IntegerType | LongType   => 0
    case DoubleType | FloatType   => 0.0
    case BooleanType              => false
    case _                        => ""
  }
}
