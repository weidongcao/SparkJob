package main.scala.ML

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext


/**
  * Created by jxl on 2016/12/26.
  */
object HdfsToHbase extends Serializable {

  //Hbase信息
  val TABLE_NAME = "H_CLUSTER_RESULT";
  //列族
  val CF = "CLUSTER_RESULT";
  //HDFS地址
  val INPUT = "hdfs://dn1:8020/Kmeans/part*";


  def main(args: Array[String]) {
    //    val sconf = new SparkConf().setAppName("indexServiceInfo")
    //    val sc = new SparkContext(sconf)
    val sc = new SparkContext("local", "SparkOnHDFS")

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //设置zookeeper集群
    conf.set("hbase.zookeeper.quorum", "nn1,nn2,dn1,dn2,dn3")
    //设置HMatser
    conf.set("hbase.zookeeper.master", "dn3:60000")


    // ======Save RDD to HBase========
    // step 1: JobConf setup
    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME)


    try {

      val dateFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      // step 3: read RDD data from somewhere and convert
      val readFile = sc.textFile(INPUT)
      println("开始索引时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + dateFormater.format(new Date) + "索引总条目：<<<<<<<<<<<<<<<<<<<<<<<<" + readFile.count())

      val localData = readFile.map(convert)
      //step 4: use 'saveAsHadoopDataset' to save RDD to HBase
      localData.saveAsHadoopDataset(jobConf)

      println("结束索引时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + dateFormater.format(new Date))

    } catch {
      case e: Exception => println(e.printStackTrace()); System.exit(-1)
      case unknown => println("Unknown exception " + unknown); System.exit(-1)
    } finally {
      //清除jobConf
      jobConf.clear()
      //关闭SparkContext上下文
      sc.stop()
    }


  }

  // step 2: rdd mapping to table
  def convert(fields: String) = {

    val field = fields.substring(1, fields.length - 1).split(",")

    val p = new Put(Bytes.toBytes(field(0)))
    p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("DATE_TIME"), Bytes.toBytes(field(1)))
    p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("K_10"), Bytes.toBytes(field(2)))
    p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("K_100"), Bytes.toBytes(field(3)))
    p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("K_1000"), Bytes.toBytes(field(4)))

    (new ImmutableBytesWritable, p)
  }


}

