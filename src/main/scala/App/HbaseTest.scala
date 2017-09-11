package main.scala.App

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * Created by Administrator on 2016/8/12.
  */
object HbaseTest {


  def readData(table: Table): Unit = {
    //查询某条数据
    val g = new Get("83540c57b01248779b23ae055a4182df".getBytes)
    val result = table.get(g)
    val SERVICEID = Bytes.toString(result.getValue("SERVICE_INFO".getBytes, "SERVICEID".getBytes))
    val SERVICE_NAME = Bytes.toString(result.getValue("SERVICE_INFO".getBytes, "SERVICE_NAME".getBytes))
    println("GET id>>> :" + SERVICEID+">>>>>>>>>>>>>"+SERVICE_NAME)

  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Program Files\\Java\\hadoop-2.6.2");

    //val sc = new SparkContext(args(0), "HBaseTest",
    //System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    //val sc = new SparkContext("local","SparkHbase");

    val conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("hbase.zookeeper.quorum", "nn1.hadoop.com");

    val conn = ConnectionFactory.createConnection(conf);
    val admin = conn.getAdmin;
    val userTable = TableName.valueOf("H_SERVICE_INFO")



    try {
      //获取 user 表
      val table = conn.getTable(userTable)

      try {

        //查询一条数据
        readData(table);

      } finally {
        if (table != null) table.close()
      }

    } finally {
      conn.close()
    }
  }



}
