package main.scala.sparkSQL

/**
  * Created by jxl on 2016/11/22.
  */

import java.io.Serializable
import java.util.logging.Logger

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlTest extends Serializable {

  case class ENDING_TRACE(ID: String, CAPTURE_TIME: String, CERTIFICATE_CODE: String, CERTIFICATE_TYPE: String, DIST: String,
                          ENDING_MAC: String, EQUIPMENT_ID: String, FIELD_STRENGTH: String, IMPORT_TIME: String, SERVICE_CODE: String)

  case class KMEANS_RESULT(ENDING_MAC: String, DATE_TIME: String, K_10: String, K_100: String, K_1000: String)


  val logger = Logger.getLogger(MySparkSql.getClass.getName)

  def main(args: Array[String]) {


    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.6.2")

    val sconf = new SparkConf()
      .setMaster("local")
      //.setMaster("spark://h230:7077")//在集群测试下设置,h230是我的hostname，须自行修改
      .setAppName("MySparkSql") //win7环境下设置
      .set("spark.executor.memory", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .setJars(jars)//在集群测试下，设置应用所依赖的jar包
    val sc = new SparkContext(sconf)

    val hconf = HBaseConfiguration.create()

    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    //设置zookeeper集群
    hconf.set("hbase.zookeeper.quorum", "nn1,nn2,dn1,dn2,dn3")
    //设置HMatser
    hconf.set("hbase.zookeeper.master", "dn3:60000")

    hconf.set(TableInputFormat.INPUT_TABLE, "H_CLUSTER_RESULT")

    //    Scan操作
    val hBaseRDD = sc.newAPIHadoopRDD(hconf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    /*val ending_trace = hBaseRDD.map(r =>
      ENDING_TRACE(
        Bytes.toString(r._2.getRow),
        check_field(Bytes.toString(r._2.getValue("SCAN_ENDING_TRACE".getBytes, "CAPTURE_TIME".getBytes))),
        check_field(Bytes.toString(r._2.getValue("SCAN_ENDING_TRACE".getBytes, "CERTIFICATE_CODE".getBytes))),
        check_field(Bytes.toString(r._2.getValue("SCAN_ENDING_TRACE".getBytes, "CERTIFICATE_TYPE".getBytes))),
        check_field(Bytes.toString(r._2.getValue("SCAN_ENDING_TRACE".getBytes, "DIST".getBytes))),
        check_field(Bytes.toString(r._2.getValue("SCAN_ENDING_TRACE".getBytes, "ENDING_MAC".getBytes))),
        check_field(Bytes.toString(r._2.getValue("SCAN_ENDING_TRACE".getBytes, "EQUIPMENT_ID".getBytes))),
        check_field(Bytes.toString(r._2.getValue("SCAN_ENDING_TRACE".getBytes, "FIELD_STRENGTH".getBytes))),
        check_field(Bytes.toString(r._2.getValue("SCAN_ENDING_TRACE".getBytes, "IMPORT_TIME".getBytes))),
        check_field(Bytes.toString(r._2.getValue("SCAN_ENDING_TRACE".getBytes, "SERVICE_CODE".getBytes)))
      )
    )*/

    val kmeans_result = hBaseRDD.map(r =>
      KMEANS_RESULT(
        Bytes.toString(r._2.getRow),
        check_field(Bytes.toString(r._2.getValue("RESULT".getBytes, "DATE_TIME".getBytes))),
        check_field(Bytes.toString(r._2.getValue("RESULT".getBytes, "K_10".getBytes))),
        check_field(Bytes.toString(r._2.getValue("RESULT".getBytes, "K_100".getBytes))),
        check_field(Bytes.toString(r._2.getValue("RESULT".getBytes, "K_1000".getBytes)))
      )
    )


    kmeans_result.take(100).foreach(println)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val Schema = sqlContext.createDataFrame(kmeans_result)

    Schema.registerTempTable("H_CLUSTER_RESULT")

    //val result = sqlContext.sql("SELECT * FROM ENDING_TRACE WHERE ENDING_MAC like '%%'")
    //val result = sqlContext.sql("SELECT * FROM KMEANS_RESULT WHERE ENDING_MAC = '28-FA-A0-A5-76-C3' ")
    val result = sqlContext.sql("select ENDING_MAC, weight_k_10 + weight_k_100 + weights_k_1000 as total_weights from  ( select ENDING_MAC, 0.05 as weights_k_10 from H_CLUSTER_RESULT  where k_10 in ( select k_10  from H_CLUSTER_RESULT  where ENDING_MAC = '00-00-00-01-4D-A0') and ENDING_MAC != '00-00-00-01-4D-A0'  ) a  left join  ( select ENDING_MAC, 0.2 as weights_k_100 from H_CLUSTER_RESULT  where k_100 in ( select k_100  from H_CLUSTER_RESULT  where ENDING_MAC = '00-00-00-01-4D-A0') and ENDING_MAC != '00-00-00-01-4D-A0'  ) b  on a.ENDING_MAC = b.ENDING_MAC  left join  ( select ENDING_MAC, 0.3 as weights_k_1000 from H_CLUSTER_RESULT  where k_1000 in ( select k_1000  from H_CLUSTER_RESULT  where ENDING_MAC = '00-00-00-01-4D-A0') and ENDING_MAC != '00-00-00-01-4D-A0' ) c  on a.ENDING_MAC = c.ENDING_MAC  order by total_weights desc")

    result.show()



    //result.collect().foreach(println)

    /*result = sqlContext.sql("SELECT sum(chinese) FROM bbs")
    result.collect().foreach(println)
    result = sqlContext.sql("SELECT avg(chinese) FROM bbs")
    result.collect().foreach(println)

    result = sqlContext.sql("SELECT sum(math) FROM bbs")
    result.collect().foreach(println)
    result = sqlContext.sql("SELECT avg(math) FROM bbs")
    result.collect().foreach(println)

    result = sqlContext.sql("SELECT sum(english) FROM bbs")
    result.collect().foreach(println)
    result = sqlContext.sql("SELECT avg(english) FROM bbs")
    result.collect().foreach(println)*/
  }

  def check_field(field: String): String = {
    if (StringUtils.isNotBlank(field)) {
      return field;
    }
    return "";
  }


}
