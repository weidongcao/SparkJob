package main.scala.App

import java.util.Date

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxl on 2016/10/12.
  */
object HBaseNewApi {



  //Hbase信息 main.scala.App.HBaseNewApi
  val TABLE_NAME = "H_CLUSTER_RESULT";
  val CF = "CLUSTER_RESULT";

  def main(args: Array[String]): Unit = {

    //定义 HBase 的配置
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    hconf.set("hbase.zookeeper.quorum", "nn1.hadoop.com")

    //设置查询的表名
    hconf.set(TableInputFormat.INPUT_TABLE, TABLE_NAME)

    //初始化SparkConf
    //val sconf = new SparkConf().setAppName("indexSolr")

    val conf = new SparkConf().setAppName("Kmeans").setMaster("local")

    val sc = new SparkContext(conf)

    //初始化SparkContext上下文
    //val sc = new SparkContext(sconf)

    //通过rdd构建索引
    val hbaseRDD = sc.newAPIHadoopRDD(
      hconf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    try {
      hbaseRDD.cache();
      println("开始时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + new Date() + "总条目：<<<<<<<<<<<<<<<<<<<<<<<<" + hbaseRDD.count())

      //遍历输出
    hbaseRDD.foreachPartition(datas => {

      datas.foreach(result => {
        //println("ID:"+Bytes.toString(result._2.getRow))
        //println("USER_NAME:"+Bytes.toString(result._2.getValue(CF.getBytes, "USER_NAME".getBytes)))

        val cells = result._2.rawCells();

        for(cell <- cells){
            System.out.print("行健: "+new String(CellUtil.cloneRow(cell)));
            System.out.print("列簇: "+new String(CellUtil.cloneFamily(cell)));
            System.out.print(" 列: "+new String(CellUtil.cloneQualifier(cell)));
            System.out.print(" 值: "+new String(CellUtil.cloneValue(cell)));
            System.out.println("时间戳: "+cell.getTimestamp());
        }


      })

    })




    println("结束时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + new Date())

  } catch {
      case e: Exception => println(e.printStackTrace()); System.exit(-1)
      case unknown => println("Unknown exception " + unknown); System.exit(-1)
    } finally {
      //关闭SparkContext上下文
      sc.stop()
    }



  }

}
