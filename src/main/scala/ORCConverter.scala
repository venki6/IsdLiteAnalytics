import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object ORCConverter {

  def main(args: Array[String]): Unit = {
    val warehouseLocation = "C:/scala-workspace/IsdAnalytics/spark-warehouse"
    val spark = SparkSession.builder
      .master("local")
      .appName("IsdAnalytics")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext

    val stationDfArray = readRawData(spark, sc)
    writeToORC(spark, stationDfArray)

    spark.stop()
  }

  def readRawData(spark: SparkSession, sc: SparkContext):Array[DataFrame] = {
    case class DataUnit(year: Integer, month: Integer, day: Integer, hour: Integer, temperature: Integer,
                        dew: Integer, seaLevel: Integer, windDir: Integer, windSpeed: Integer, sky: Integer,
                        prepOne: Integer, prepSix: Integer )

    // read raw data for each station, this approach is easy to scala to more stations
    val rawData010 = sc.textFile("C:/scala-workspace/IsdAnalytics/data/201*/010010-99999-201*")
    val rawData014 = sc.textFile("C:/scala-workspace/IsdAnalytics/data/201*/010014-99999-201*")
    val rawData020 = sc.textFile("C:/scala-workspace/IsdAnalytics/data/201*/010020-99999-201*")
    val rawData030 = sc.textFile("C:/scala-workspace/IsdAnalytics/data/201*/010030-99999-201*")
    val rawData060 = sc.textFile("C:/scala-workspace/IsdAnalytics/data/201*/010060-99999-201*")

    //defining schema for fixed width file
    val schemaString = "year,month,day,hour,temperature,dew,seaLevel,windDir,windSpeed,skyCoverage,prepOne,prepSix"
    val fields = schemaString.split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // convert RDD to Dataframe
    //add usaf and wban as columns
    val df010 = spark.createDataFrame(rawData010.map(x => getRow(x)), schema)
        .withColumn("usaf", lit("010010"))
        .withColumn("wban", lit("99999"))
    val df014 = spark.createDataFrame(rawData014.map(x => getRow(x)), schema)
      .withColumn("usaf", lit("010014"))
      .withColumn("wban", lit("99999"))
    val df020 = spark.createDataFrame(rawData020.map(x => getRow(x)), schema)
      .withColumn("usaf", lit("010020"))
      .withColumn("wban", lit("99999"))
    val df030 = spark.createDataFrame(rawData030.map(x => getRow(x)), schema)
      .withColumn("usaf", lit("010030"))
      .withColumn("wban", lit("99999"))
    val df060 = spark.createDataFrame(rawData060.map(x => getRow(x)), schema)
      .withColumn("usaf", lit("010060"))
      .withColumn("wban", lit("99999"))
    df060.printSchema()
    df060.take(10).foreach(println)

    var stationDfs = new Array[DataFrame](5)
    stationDfs = Array(df010, df014, df020, df030, df060)
    stationDfs
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
    columnArray(6)=x.substring(25,31).trim //sea level
    columnArray(7)=x.substring(31,37).trim //wind direction
    columnArray(8)=x.substring(37,43).trim //wind speed rate
    columnArray(9)=x.substring(43,49).trim //sky coverage
    columnArray(10)=x.substring(49,55).trim //one hour precipitation
    columnArray(11)=x.substring(55).trim //six hour precipitation
    Row.fromSeq(columnArray)
  }

  def writeToORC(spark: SparkSession, stationDfArray: Array[DataFrame]) = {
    for(i <- 0 until stationDfArray.length){

      stationDfArray(i).write
        .option("compression","none") // disabled compression, file size is small
        .mode(SaveMode.Append) // Append mode is crucial to enable scaling on input station range
        .partitionBy("year", "usaf") // easy for both year and per-station aggregations
        //.bucketBy(12,"month")
        .orc("C:/scala-workspace/IsdAnalytics/output/orc/") //ORC format is better suited for analytics
    }
  }

}
