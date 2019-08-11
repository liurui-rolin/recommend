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
    // 创建spark
    val spark = SparkSession.builder.master("local[2]").appName("Hot base app").getOrCreate()
    //读取数据
    val logData = spark.read.textFile(logFile)
    import spark.implicits._
    //简单清洗
    val etlData = logData.map(_.toString.replace("[","").replace("]","")).cache()

    //显示示例数据
    etlData.limit(10).collect().foreach(println(_))

    //执行热词计算
    val hotWords = etlData.map(line => (line.split("\t")(2),1)).rdd.reduceByKey((a,b) => a+b).map(res => (res._2,res._1)).sortByKey(false,1)
    hotWords.take(100).foreach(println(_))


    spark.stop()
    println("end...")
  }
}