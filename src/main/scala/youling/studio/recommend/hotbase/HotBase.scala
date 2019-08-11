package youling.studio.recommend.hotbase

import org.apache.spark.sql.SparkSession

/**
  * @author liurui
  * @date 2019/8/11 下午9:32
  */
object HotBase {
  def main(args: Array[String]): Unit = {
    println("start...")

    val logFile = "data/SogouQ.sample"
    val spark = SparkSession.builder.master("local[2]").appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile)
    import spark.implicits._
    val etlData = logData.map(_.toString.replace("[","").replace("]",""))

    etlData.limit(10).collect().foreach(println(_))


    spark.stop()
    println("end...")
  }
}
