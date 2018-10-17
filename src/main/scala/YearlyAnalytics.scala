object YearlyAnalytics {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = "C:/scala-workspace/IsdAnalytics/spark-warehouse"
    val spark = SparkSession.builder
      .master("local")
      .appName("IsdAnalytics")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
  }

}
