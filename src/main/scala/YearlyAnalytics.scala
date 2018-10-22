import exceptions.InvalidYearRange
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object YearlyAnalytics {

  def main(args: Array[String]): Unit = {

    <!-- bad parameters are handled
    parameters are handled before session creation -->
    val inputParams = Array("2018", "2018")
    val yearRange = validateParameters(inputParams)

    <!-- create spark context with hive support-->
    val warehouseLocation = "C:/git/IsdLiteAnalytics/sqlWarehouse"
    val spark = SparkSession.builder
      .master("local")
      .appName("YearlyAnalytics")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    val orcDFArray = createYearlyDf(spark, yearRange)

    executeYearlyTemperatureAnalytics(spark, orcDFArray, yearRange)

//    val yearDf: Try[DataFrame] = readYearlyData(spark: SparkSession, sc: SparkContext, "2015")
//    System.out.println("count for ORC DF " + yearDf.get.count())
  }

  <!-- using try-success-failure monads to effectively handle all input types -->
  def validateParameters(args: Array[String]):Array[Int] = {
    val yearRange: Try[Array[Int]] = Try(Array.range(args(0).toInt, args(1).toInt + 1))
    yearRange match {
      case Failure(ex) => {
        println(s"Invalid date range, exception msg: $ex")
        throw ex
      }
      case Success(value) => {
        if(yearRange.get.size < 1){
          throw new InvalidYearRange("Year range size is less than 1")
        }else{
          for(x <- value) println(x)
          value
        }
      }
    }
  }

  def createYearlyDf(spark: SparkSession, yearRange: Array[Int]):Array[DataFrame] = {
    import spark.implicits._
    var orcDFArray = ArrayBuffer[DataFrame]()
    for(i <- yearRange){
      var yearString = i.toString
      readYearlyData(spark, yearString) match {
        case Success(validInputDF) => {

          <!-- val fields = Array(
            StructField("month", IntegerType, nullable = false),
            StructField("day", IntegerType, nullable = false),
            StructField("hour", IntegerType, nullable = false),
            StructField("temperature", StringType, nullable = true),
            StructField("dew", StringType, nullable = true),
            StructField("seaLevel", StringType, nullable = true),
            StructField("windDir", StringType, nullable = true),
            StructField("windSpeed", StringType, nullable = true),
            StructField("skyCoverage", StringType, nullable = true),
            StructField("prepOne", StringType, nullable = true),
            StructField("prepSix", StringType, nullable = true),
            StructField("wban", StringType, nullable = true),
            StructField("usaf", IntegerType, nullable = false))
          val schema = StructType(fields) -->
          orcDFArray += validInputDF
        }
        case Failure(ex) => println("Invalid input File path: " + ex.getMessage)
      }
    }
    orcDFArray.toArray
  }
  <!-- take year as string
        returns Try[YearDataframe] - for better error handling -->
  def readYearlyData(spark: SparkSession, year: String): Try[DataFrame] = {
    val sc = spark.sparkContext
    spark.sql("SET spark.sql.orc.enabled=true") //enable vectorization for faster reads

    Try(spark.read
        .format("orc")
        .load("C:/git/IsdLiteAnalytics/orcData/" + "year=" + year)
    )
  }

  case class TemperatureDataUnit(month: Int, day: Int, hour: Int, temperature: Option[Int], wban: String,
                                 usaf: Int)
  case class AbsoluteTemperatureUnit(month: Int, day: Int, hour: Int, temperature: Int, wban: String,
                                     usaf: Int)

  def executeYearlyTemperatureAnalytics(spark: SparkSession, orcDFArray: Array[DataFrame], yearRange: Array[Int]): Unit = {
    import spark.implicits._
    spark.catalog.listTables().show()

    for(orcYearDF <- orcDFArray; i <- yearRange){
      orcYearDF.createOrReplaceTempView("orcYearDF")
      var yearString = i.toString
      val absoluteYearDS = orcYearDF
        //map function to convert String temperature to Int,
        //using option/some/none monads for conversion
        .map(s => TemperatureDataUnit(s.getInt(0), s.getInt(1), s.getInt(2),
          Option(s.getString(3).toInt), s.getString(11), s.getInt(12)))
        // filter data with missing temperature reading
        .filter(t => t.temperature match {
          case Some(x) => {
            if(x == -9999) false
            else true
          }
          case None => false
          })
        // map the filterd object to Absolute Temperature Unit, remove type option for Temperature column
        .map(s => AbsoluteTemperatureUnit(s.month, s.day, s.hour, s.temperature.get, s.wban, s.usaf))
        .toDF("month", "day", "hour", "temperature", "wban", "usaf")

      absoluteYearDS.printSchema()
      absoluteYearDS.take(5).foreach(println)

//      val windowSpec = Window.partitionBy($"usaf").orderBy($"temperature".desc)
//      absoluteYearDS
//        .toDF()
//        .withColumn("rank", functions.rank().over(windowSpec))
//        .printSchema()

      absoluteYearDS.createOrReplaceTempView("abtYearDF")
//    spark.sql("select * from (select *, rank() over (partition by usaf order by temperature desc) as rank from orcYearDF) ranked_yearDF where ranked_yearDF.rank = 1").show()
      spark.sql(s"select $yearString, usaf, max(temperature) as max_temperature from abtYearDF group by usaf").show()
    }

  }

}
