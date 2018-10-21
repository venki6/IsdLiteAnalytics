import exceptions.InvalidInputFilePath
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.typechecker.Macros
import scala.util.{Failure, Success, Try}

object ORCConverter {

  def main(args: Array[String]): Unit = {
    val warehouseLocation = "C:/git/IsdLiteAnalytics/sqlWarehouse"
    val spark = SparkSession.builder
      .master("local")
      .appName("IsdAnalytics")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext

    val yearList = Array[Int](2016)
    val stationIDList = Array[String]("010010-99999","010014-99999","010020-99999","010030-99999","010060-99999")

    val validInputDFArray = readRawDataFiles(spark, sc, yearList, stationIDList)
    println("Count on valid input dataframes = " + validInputDFArray.size)

    filterInvalidData(validInputDFArray)

    //writeToORC(spark, validInputDFArray)

    spark.stop()
  }

  def readRawDataFiles(spark: SparkSession, sc: SparkContext, yearList: Array[Int],
                       stationList: Array[String]):Array[DataFrame] = {

    <!--using flat map to create file paths for all input files
        return type is set to Seq, we can create a RDD and parallelize data read-write processes, if needed -->
    val inputPathList: Seq[String] = yearList.flatMap {
      x => stationList.map(y => "C:/git/IsdLiteAnalytics/rawData/" + x + "/" + y + "-" + x)
    }
    for(x <- inputPathList) println(x.toString)

    var inputDFArray = ArrayBuffer[Try[DataFrame]]()
    <!-- loop through file path list and create DF for each path-->
    for(x <- inputPathList)
      inputDFArray += createInputFileDF(spark, x)

    <!-- -->
    var validInputDFArray = ArrayBuffer[DataFrame]()
    for(i <- inputDFArray){
      i match {
        case Success(value) => {
          validInputDFArray += value
        }
        case Failure(value) => println("Failure DataFrame found: " + value.toString)
       }
    }
    validInputDFArray.toArray
  }
  <!-- return type: Try[DataFrame], process would continue even if one of the input path does not exist -->
  def createInputFileDF(spark: SparkSession, filePath:String): Try[DataFrame] = {
    val sc = spark.sparkContext
    try{
      val fileName = filePath.substring(filePath.lastIndexOf("/")+1)
      val fileNameArray = fileName.split("-")
      val usafID:String = fileNameArray(0)
      val wbanId = fileNameArray(1)
      <!-- using Try monads to create RDD-->
      val inputFileRDD: Try[RDD[String]] = Try(sc.textFile(filePath))
      <!-- define schema for fixed width file-->
      val schemaString = "year,month,day,hour,temperature,dew,seaLevel,windDir,windSpeed,skyCoverage,prepOne,prepSix"
      val fields = schemaString.split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val schema = StructType(fields)

      inputFileRDD match {
        case Success(validFileRDD) =>{
          <!-- forcing action on RDD to evaluate it -->
          validFileRDD.count()
          <!-- using Try monads to construct dataframe
          add station info to individual record, data enhancment -->
          val inputFileDF: Try[DataFrame] = Try(spark.createDataFrame(validFileRDD.map(x => getRow(x)), schema)
            .withColumn("usaf", lit(usafID))
            .withColumn("wban", lit(wbanId)))
          inputFileDF
        }
      }
    }catch {
      case ex: Exception =>{
        println("File path provided for RDD does not exist: " + filePath.toString)
        Failure(new InvalidInputFilePath("Invalid input file path:" + filePath + ", Exception message: "
                + ex.getMessage))
      }
    }
  }

  //method to map fixed length columns to names
  def getRow(x : String) : Row={
    val columnArray = new Array[String](12)
    columnArray(0)=x.substring(0,4).trim //year
    columnArray(1)=x.substring(5,7).trim //month
    columnArray(2)=x.substring(8,11).trim //day
    columnArray(3)=x.substring(11,13).trim //hour
    columnArray(4)=x.substring(13,19).trim //temperature
    columnArray(5)=x.substring(19,25).trim //dew **
    columnArray(6)=x.substring(25,31).trim //sea level pressure
    columnArray(7)=x.substring(31,37).trim //wind direction
    columnArray(8)=x.substring(37,43).trim //wind speed rate
    columnArray(9)=x.substring(43,49).trim //sky coverage
    columnArray(10)=x.substring(49,55).trim //one hour precipitation
    columnArray(11)=x.substring(55).trim //six hour precipitation
    Row.fromSeq(columnArray)
  }

  case class DataUnit(year: Int, moth: Int, day: Int, hour: Int, temperature: Int, dew: Int, seaLevel: Int,
                      windDirection: Int, windSpeed: Int, skyCoverage: Int, oneHrPrep: Int, sixHrPrep: Int)

  def filterInvalidData(validInputDFArray: Array[DataFrame]) = {

    for(x <- validInputDFArray){

    }
  }

  def writeToORC(spark: SparkSession, stationDfArray: Array[DataFrame]) = {
    for(i <- 0 until stationDfArray.length){

      stationDfArray(i).write
        .option("compression","none") // disabled compression, file size is small
        .mode(SaveMode.Append) // Append mode is crucial to enable scaling on input station range
        .partitionBy("year", "usaf") // easy for both year and per-station aggregations
        //.bucketBy(12,"month") //future enhancment
        .orc("C:/git/IsdLiteAnalytics/orcData/") //ORC format is better suited for analytics
    }
  }

}
