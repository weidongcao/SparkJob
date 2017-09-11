package main.scala.App

import org.apache.spark.SparkContext

/**
  * Created by jxl on 2017/2/27.
  */
object WordCount {

  val INPUT = "input/SearchFieldsResult"

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.6.2")
    val sc = new SparkContext("local", "SparkWordCount")
    val line = sc.textFile(INPUT)
    line.map((_, 1)).reduceByKey(_ + _).collect().foreach(println)//.flatMap(_.split(" "))
    sc.stop()
  }


}
