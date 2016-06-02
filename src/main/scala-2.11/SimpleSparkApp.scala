/**
  * Created by mamonu on 19/05/2016.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.databricks.spark.csv
import org.apache.spark.sql.SQLContext


object SimpleSparkApp {




  def main(args: Array[String]) {
    val logFile = "file:///home/mamonu/scripts/hellospark/LCF.csv" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("bread")).count()
    val numBs = logData.filter(line => line.contains("cheese")).count()
    println("Lines with Hamlet: %s, Lines with Othello %s".format(numAs, numBs))

    val  f = sc.textFile("file:///home/mamonu/scripts/hellospark/LCF.csv")

    val wordCounts = f.flatMap(line => line.split(","))
      .map(word => (word, 1))
      .reduceByKey(_ + _, 1)  // 2nd arg configures one task (same as number of partitions)
      .map(item => item.swap) // interchanges position of entries in each tuple
      .sortByKey(true, 1) // 1st arg configures ascending sort, 2nd arg configures one task
      .map(item => item.swap).foreach(println)

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .load("file:///home/mamonu/scripts/hellospark/LCF.csv")


    df.printSchema()



    val selectedData = df.select("EXPDESC", "CODING","Paid1","Quantity","Units" )
    selectedData.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("file:///home/mamonu/scripts/hellospark/newLCF.csv")


  }









}
