package main.scala.sparkSQL

/**
  * Created by jxl on 2017/02/26.
  */

import java.io.Serializable
import java.util.logging.Logger

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object MySparkSql extends Serializable {

  val logger = Logger.getLogger(MySparkSql.getClass.getName)

  case class BBS(id: String, sessionid: String, url: String, name: String)

  def main(args: Array[String]) {


    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.6.2")

    val sconf = new SparkConf()
      .setMaster("local")
      //.setMaster("spark://h230:7077")//在集群测试下设置,h230是我的hostname，须自行修改
      .setAppName("MySparkSql") //win7环境下设置
      .set("spark.executor.memory", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.setJars(jars)//在集群测试下，设置应用所依赖的jar包
    val sc = new SparkContext(sconf)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "node1,node2,data1,data2,data3")
    conf.set("hbase.master", "data3:60000")
    //conf.addResource("/home/hadoop/SW/hbase/conf/hbase-site.xml")//替代以上三条配置信息
    conf.set(TableInputFormat.INPUT_TABLE, "BBS_TEST")

    //Scan操作
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val bbs = hBaseRDD.map(r =>
      BBS(
        Bytes.toString(r._2.getRow),
        check_field(Bytes.toString(r._2.getValue("CF".getBytes, "SESSIONID".getBytes))),
        check_field(Bytes.toString(r._2.getValue("CF".getBytes, "NAME".getBytes))),
        check_field(Bytes.toString(r._2.getValue("CF".getBytes, "URL".getBytes)))
      )
    )


    bbs.foreach(println)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val bbsSchema = sqlContext.createDataFrame(bbs)
    bbsSchema.registerTempTable("BBS")

    val result = sqlContext.sql("SELECT * FROM BBS WHERE name like '%常宁%'")

    result.show()

  }

  def check_field(field: String): String = {
    if (StringUtils.isNotBlank(field)) {
      return field;
    }
    return "";
  }


}
