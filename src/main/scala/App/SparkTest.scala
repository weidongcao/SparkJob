package main.scala.App

import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/8/12.
  */
object SparkTest extends Serializable {

  def main(args: Array[String]): Unit = {

    //Hbase信息
    val TABLE_NAME = "BBS_TEST";
    val CF = "CF";

    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.6.2")

    val conf = new SparkConf().setAppName("sparktest").setMaster("local");
    val sc = new SparkContext(conf);

    val readFile = sc.textFile("input/1466645280-16218-service_info.bcp").map(line => line.split("""\|#\|"""))

    readFile.foreachPartition {
      x => {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "dn2.hadoop.com")
        myConf.set("hbase.zookeeper.property.clientPort", "2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, TableName.valueOf(TABLE_NAME))
        myTable.setAutoFlush(false, false) //关键点1
        myTable.setWriteBufferSize(3 * 1024 * 1024) //关键点2
        x.foreach { y => {
          println(y(0) + ":::" + y(1))
          var p = new Put(Bytes.toBytes(y(0)))
          p.add(Bytes.toBytes(CF), Bytes.toBytes("SESSIONID"), Bytes.toBytes(y(1)));
          p.add(Bytes.toBytes(CF), Bytes.toBytes("URL"), Bytes.toBytes(y(2)));
          p.add(Bytes.toBytes(CF), Bytes.toBytes("NAME"), Bytes.toBytes(y(3)));
          myTable.put(p)
          p = null
        }
        }
        myTable.flushCommits() //关键点3
      }
    }


    /*//Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    //val conn = ConnectionFactory.createConnection(myConf)
    val myConf = HBaseConfiguration.create()
    myConf.set("hbase.zookeeper.quorum", "dn2.hadoop.com")
    myConf.set("hbase.zookeeper.property.clientPort", "2181")
    myConf.set("hbase.defaults.for.version.skip", "true")
    val myTable = new HTable(myConf, TableName.valueOf(TABLE_NAME))
      //val myTable = conn.getTable(TableName.valueOf(TABLE_NAME))
      myTable.setWriteBufferSize(3 * 1024 * 1024) //关键点2
      //获取 user 表
      myTable setAutoFlush(false, false) //关键点1

      readFile.foreachPartition {

        fields => {


          fields.foreach { field => {

            println(field(0) + ":::" + field(1) + ":::" + field(2) + ":::" + field(3))

            val p = new Put(Bytes.toBytes(field(0)))
            //p.add(Bytes.toBytes(CF), Bytes.toBytes("SESSIONID"), Bytes.toBytes(y(1)));
            p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("SESSIONID"), Bytes.toBytes(field(1)))
            p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("URL"), Bytes.toBytes(field(2)))
            p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("NAME"), Bytes.toBytes(field(3)))

            myTable.put(p)
            //p = null
          }
          }
          myTable.flushCommits() //关键点3
        }
      }

    /*} finally {
      myTable.close()
      sc.stop()
    }*/*/


  }
}
