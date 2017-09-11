package main.scala.tmpJob

import java.text.{ParseException, SimpleDateFormat}
import java.util.{ArrayList, Date, UUID}

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Search {

  //Hbase信息
  val TABLE_NAME = "H_REG_CONTENT_SEARCH_TMP";
  val CF = "CONTENT_SEARCH";

  //solr客户端
  //val client = new HttpSolrClient.Builder("http://localhost:8983/solr/yisou").build();
  //val solrClient = new HttpSolrClient.Builder("http://dn1.hadoop.com:8983/solr/yisou").build();

  //val zkHost = "dn1.hadoop.com:2181,dn2.hadoop.com:2181,dn3.hadoop.com:2181,nn1.hadoop.com:2181,nn2.hadoop.com:2181"
  val zkHost = "nn1:2181,nn2:2181,dn1:2181,dn2:2181,dn3:2181"
  val defaultCollection = "yisou"
  val cloudBuilder = new CloudSolrClient.Builder
  val solrClient = cloudBuilder.withZkHost(zkHost).build()
  solrClient.setDefaultCollection(defaultCollection)

  //批量提交的条数
  val batchCount: Int = 5000

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.6.2")

    //定义 HBase 的配置
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    //hconf.set("hbase.zookeeper.quorum", "nn1.hadoop.com")
    hconf.set("hbase.zookeeper.quorum", "nn1,nn2,dn1,dn2,dn3")
    hconf.set("hbase.zookeeper.master", "dn2:60000")

    //设置查询的表名
    hconf.set(TableInputFormat.INPUT_TABLE, TABLE_NAME)


    //远程提交时，需要提交打包后的jar
    //val jarPath = "out\\artifacts\\SparkAction_jar\\SparkAction.jar"
    //val jarPath = "/usr/local/SparkAction.jar";

    //out\\artifacts\\SparkAction_jar\\SparkAction.jar
    //远程提交时，伪装成相关的hadoop用户，否则，可能没有权限访问hdfs系统
    //System.setProperty("user.name", "hdfs");
    //val conf = new SparkConf().setMaster("spark://192.168.1.187:7077").setAppName("build index ");

    //初始化SparkConf
    //val sconf = new SparkConf().setAppName("indexWeibo").setMaster("local")
    val sconf = new SparkConf().setAppName("indexSearch_tmp")
    //--- spark://nn1.hadoop.com:7070

    //上传运行时依赖的jar包D:\hadoop\lib
    //val seq = Seq(jarPath) :+ "D:\\hadoop\\lib\\noggit-0.6.jar" :+ "D:\\hadoop\\lib\\httpclient-4.5.2.jar" :+ "D:\\hadoop\\lib\\httpcore-4.4.5.jar" :+ "D:\\hadoop\\lib\\solr-solrj-6.1.0.jar" :+ "D:\\hadoop\\lib\\httpmime-4.5.2.jar"
    //val seq = Seq(jarPath) :+ "/opt/cloudera/parcels/CDH-5.6.1-1.cdh5.6.1.p0.3/jars/noggit-0.6.jar" :+ "/opt/cloudera/parcels/CDH-5.6.1-1.cdh5.6.1.p0.3/jars/httpclient-4.5.2.jar" :+ "/opt/cloudera/parcels/CDH-5.6.1-1.cdh5.6.1.p0.3/jars/httpcore-4.4.5.jar" :+ "/opt/cloudera/parcels/CDH-5.6.1-1.cdh5.6.1.p0.3/jars/solr-solrj-6.1.0.jar" :+ "/opt/cloudera/parcels/CDH-5.6.1-1.cdh5.6.1.p0.3/jars/httpmime-4.5.2.jar"
    //sconf.setJars(seq)

    //初始化SparkContext上下文
    val sc = new SparkContext(sconf)

    //通过rdd构建索引
    val hbaseRDD = sc.newAPIHadoopRDD(
      hconf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    try {
      //清空全部索引!
      //deleteSolrByQuery("*:*");
      commitSolr(hbaseRDD)
    } catch {
      case e: Exception => println(e.printStackTrace()); System.exit(-1)
      case unknown => println("Unknown exception " + unknown); System.exit(-1)
    } finally {
      //关闭索引资源
      solrClient.close()
      //关闭SparkContext上下文
      sc.stop()
    }

  }

  def commitSolr(hbaseRDD: RDD[(ImmutableBytesWritable, Result)]) {

    val cacheList = new ArrayList[SolrInputDocument]();

    hbaseRDD.cache();

    println("开始时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + new Date() + "总条目：<<<<<<<<<<<<<<<<<<<<<<<<" + hbaseRDD.count())

    //遍历输出
    hbaseRDD.foreachPartition(datas => {
      datas.foreach(result => {
        var sDoc = new SolrInputDocument();
        transFormDocs(result._2, sDoc);
        cacheList.add(sDoc)
        sDoc = null;
        if (!cacheList.isEmpty && cacheList.size() == batchCount) {
          solrClient.add(cacheList, 10000); //缓冲10s提交数据
          cacheList.clear(); //清空集合，便于重用
        }
      })
      if (!cacheList.isEmpty) solrClient.add(cacheList, 10000); //提交数据
    })
    println("结束时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + new Date())
  }

  def transFormDocs(result: Result, sDoc: SolrInputDocument) {

    //sDoc.addField("ID", Bytes.toString(result.getRow)); //---rowKey
    val uuid = UUID.randomUUID().toString.replace("-", "")
    sDoc.addField("ID", uuid); //---rowKey
    sDoc.addField("docType", "搜索");

    sDoc.addField("SID", Bytes.toString(result.getRow)); //---rowKey
    sDoc.addField("SESSIONID", etl_field(Bytes.toString(result.getValue(CF.getBytes, "SESSIONID".getBytes))));
    sDoc.addField("SERVICE_CODE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "SERVICE_CODE".getBytes))));
    sDoc.addField("ROOM_ID", etl_field(Bytes.toString(result.getValue(CF.getBytes, "ROOM_ID".getBytes))));
    sDoc.addField("CERTIFICATE_TYPE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "CERTIFICATE_TYPE".getBytes))));
    sDoc.addField("CERTIFICATE_CODE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "CERTIFICATE_CODE".getBytes))));
    sDoc.addField("USER_NAME", etl_field(Bytes.toString(result.getValue(CF.getBytes, "USER_NAME".getBytes))));
    sDoc.addField("PROTOCOL_TYPE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "PROTOCOL_TYPE".getBytes))));
    sDoc.addField("URL", etl_field(Bytes.toString(result.getValue(CF.getBytes, "URL".getBytes))));
    sDoc.addField("DOMAIN_NAME", etl_field(Bytes.toString(result.getValue(CF.getBytes, "DOMAIN_NAME".getBytes))));
    sDoc.addField("REF_URL", etl_field(Bytes.toString(result.getValue(CF.getBytes, "REF_URL".getBytes))));
    sDoc.addField("REF_DOMAIN", etl_field(Bytes.toString(result.getValue(CF.getBytes, "REF_DOMAIN".getBytes))));
    sDoc.addField("KEYWORD", etl_field(Bytes.toString(result.getValue(CF.getBytes, "KEYWORD".getBytes))));
    sDoc.addField("KEYWORD_CODE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "KEYWORD_CODE".getBytes))));
    sDoc.addField("DEST_IP", etl_field(Bytes.toString(result.getValue(CF.getBytes, "DEST_IP".getBytes))));
    sDoc.addField("DEST_PORT", etl_field(Bytes.toString(result.getValue(CF.getBytes, "DEST_PORT".getBytes))));
    sDoc.addField("SRC_IP", etl_field(Bytes.toString(result.getValue(CF.getBytes, "SRC_IP".getBytes))));
    sDoc.addField("SRC_PORT", etl_field(Bytes.toString(result.getValue(CF.getBytes, "SRC_PORT".getBytes))));
    sDoc.addField("SRC_MAC", etl_field(Bytes.toString(result.getValue(CF.getBytes, "SRC_MAC".getBytes))));
    sDoc.addField("CHECKIN_ID", etl_field(Bytes.toString(result.getValue(CF.getBytes, "CHECKIN_ID".getBytes))));
    sDoc.addField("DATA_SOURCE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "DATA_SOURCE".getBytes))));
    sDoc.addField("MACHINE_ID", etl_field(Bytes.toString(result.getValue(CF.getBytes, "MACHINE_ID".getBytes))));
    sDoc.addField("IMPORT_TIME", etl_field(Bytes.toString(result.getValue(CF.getBytes, "IMPORT_TIME".getBytes))));
    sDoc.addField("CAPTURE_TIME", etl_field(Bytes.toString(result.getValue(CF.getBytes, "CAPTURE_TIME".getBytes))));

    sDoc.addField("capture_time", strToTime(Bytes.toString(result.getValue(CF.getBytes, "CAPTURE_TIME".getBytes))));
  }

  @throws[ParseException]
  def strToTime(date: String): Long = {
    if (StringUtils.isBlank(date)) {
      return 0
    } else {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      dateFormat.parse(date).getTime;
    }
  }

  def etl_field(field: String): String = {
    if (StringUtils.isBlank(field)) {
      return "";
    } else field
  }

  def deleteSolrByQuery(query: String): Unit = {
    solrClient.deleteByQuery(query);
    solrClient.commit()
    println("数据已清空!")
  }

}
