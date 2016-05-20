/**
  * Created by mamonu on 19/05/2016.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.databricks.spark.csv



object SimpleSparkApp {




  def main(args: Array[String]) {
    val logFile = "/Users/thorosm2002/scripts/Bean.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("statistics")).count()
    val numBs = logData.filter(line => line.contains("data")).count()
    println("Lines with statistics: %s, Lines with data: %s".format(numAs, numBs))
  }









}
