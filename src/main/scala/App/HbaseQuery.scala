package main.scala.App

import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.SparkContext

/**
  * Created by Administrator on 2016/8/12.
  */
object HbaseQuery {

  val TABLE_NAME = "H_SERVICE_INFO";


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Program Files\\Java\\hadoop-2.6.2");

    val conf = HBaseConfiguration.create();



    val table = new HTable(conf,TABLE_NAME);

    //search table
    val config = HBaseConfiguration.create
    val sc = new SparkContext("local", "HBaseTest",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_TEST_JAR")))




    config.set(TableInputFormat.INPUT_TABLE, TABLE_NAME)


    // 用hadoopAPI创建一个RDD


    val hbaseRDD = sc.newAPIHadoopRDD(config, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])




    val count = hbaseRDD.count()
    println("HbaseRDD Count>>>>>>" + count)
    hbaseRDD.cache()

    //res(0)._2返回第二个参数result
    val res = hbaseRDD.take(count.toInt)


    for (j <- 1 to count.toInt) {

      var rs = res(j - 1)._2


      //遍历res.raw取出每一个单元的值  ,返回类型：Array[org.apache.hadoop.hbase.KeyValue]
      var cells = rs.rawCells();


      for (cell <- cells) //再遍历每一个单元里面的记录
        println(
          "row:" + new String(rs.getRow)) +
          " cf:" + new String(CellUtil.cloneFamily(cell)) +
          " column:" + new String(CellUtil.cloneQualifier(cell)) +
          " value:" + new String(CellUtil.cloneValue(cell))
    }


  }

}
