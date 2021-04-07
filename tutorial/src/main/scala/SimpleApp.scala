import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
      val dataLoc = "/opt/spark-2.3.4-bin-hadoop2.7/README.md"
      val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
      val logData = spark.read.textFile(dataLoc).cache()
      val numAs = logData.filter(line => line.contains("a")).count()
      val numBs = logData.filter(line => line.contains("b")).count()
      println(s"Lines with a: $numAs, Lines with b: $numBs")
      spark.stop()
  }
}