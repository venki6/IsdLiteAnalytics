import exceptions.InvalidYearRange
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

object YearlyAnalytics {

  def main(args: Array[String]): Unit = {

    val inputParams = Array("2020", "2018")
    val yearRange = validateParameters(inputParams)

    val warehouseLocation = "C:/git/IsdLiteAnalytics/sqlWarehouse"
    val spark = SparkSession.builder
      .master("local")
      .appName("YearlyAnalytics")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext

    createYearlyDf(yearRange)

//    val yearDf: Try[DataFrame] = readYearlyData(spark: SparkSession, sc: SparkContext, "2015")
//    System.out.println("count for ORC DF " + yearDf.get.count())
  }

  def validateParameters(args: Array[String]):Array[Int] = {
    //using try-success-failure monad to effectively handle all input types
    val yearRange: Try[Array[Int]] = Try(Array.range(args(0).toInt, args(1).toInt + 1))
    yearRange match {
      case Failure(ex) => {
        System.out.println(s"Invalid date range, exception msg: $ex")
        throw ex
      }
      case Success(value) => {
        if(yearRange.getOrElse(Array.empty[Int]).size < 1){
          throw new InvalidYearRange("Year range size is less than 1")
        }else{
          for(x <- value) println(x)
          value
        }
      }
    }
  }

  def createYearlyDf(yearRange: Array[Int]) = {

  }
  <!-- take year as string
        returns Try[YearDataframe] - for better error handling -->
  def readYearlyData(spark: SparkSession, sc: SparkContext, year: String): Try[DataFrame] = {
    spark.sql("SET spark.sql.orc.enabled=true") //enable vectorization for faster reads
    try{
      val yearDf = spark.read.format("orc")
        .load("C:/git/IsdLiteAnalytics/orcData/" + "year=" + year )
      Success(yearDf)
    }catch {
      case ex:AnalysisException => {
        System.out.print("No dataset present for the year requested")
        Failure(ex)
      }
      case ex: Exception => {
        Failure(ex)
      }
    }
  }
}
