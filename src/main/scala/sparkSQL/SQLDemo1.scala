package main.scala.sparkSQL

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object InferringSchema {
  def main(args: Array[String]) {

    //创建SparkConf()并设置App名称
    val conf = new SparkConf().setAppName("SQL-1")
    //SQLContext要依赖SparkContext
    val sc = new SparkContext(conf)
    //创建SQLContext
    val sqlContext = new SQLContext(sc)

    //从指定的地址创建RDD
    val lineRDD = sc.textFile(args(0)).map(_.split(" "))

    //创建case class
    //将RDD和case class关联
    val personRDD = lineRDD.map(x => Test(x(0), x(1), x(2), x(3)))
    //导入隐式转换，如果不导入无法将RDD转换成DataFrame
    //将RDD转换成DataFrame
    import sqlContext.implicits._
    val personDF = personRDD.toDF
    //注册表
    personDF.registerTempTable("BBS_TEST")
    //传入SQL
    val df = sqlContext.sql("select * from BBS_TEST order by age desc limit 2")
    //将结果以JSON的方式存储到指定位置
    df.write.json(args(1))
    //停止Spark Context
    sc.stop()
  }

}

//case class一定要放到外面
case class Test(id: String, sessionid: String, url: String, name: String)
