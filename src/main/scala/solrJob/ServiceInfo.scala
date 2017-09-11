package main.scala.solrJob

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


object ServiceInfo {

  //Hbase信息
  val TABLE_NAME = "H_SERVICE_INFO";
  val CF = "SERVICE_INFO";

  //solr客户端
  //val client = new HttpSolrClient.Builder("http://localhost:8983/solr/yisou").build();
  //val solrClient = new HttpSolrClient.Builder("http://dn1.hadoop.com:8983/solr/yisou").build();

  //val zkHost = "nn1.hadoop.com:2181,nn2.hadoop.com:2181,dn1.hadoop.com:2181,dn2.hadoop.com:2181,dn3.hadoop.com:2181"
  val zkHost = "dn1:2181,dn2:2181,dn3:2181,dn4:2181,dn5:2181,nn1:2181,nn2:2181"
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
    hconf.set("hbase.zookeeper.quorum", "nn1,nn2,dn1,dn2,dn3,dn4,dn5")
    hconf.set("hbase.zookeeper.master", "nn1:60000")

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
    val sconf = new SparkConf().setAppName("indexServiceInfo")
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
    //sDoc.addField("ID", Bytes.toString(result.getRow));
    val uuid = UUID.randomUUID().toString.replace("-", "")
    sDoc.addField("ID", uuid); //---rowKey
    sDoc.addField("docType", "场所");

    sDoc.addField("SID", Bytes.toString(result.getRow)); //---rowKey
    sDoc.addField("GROUP_ID", etl_field(Bytes.toString(result.getValue(CF.getBytes, "GROUP_ID".getBytes))));
    sDoc.addField("SERVICE_TYPE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "SERVICE_TYPE".getBytes))));
    sDoc.addField("SERVICE_NAME", etl_field(Bytes.toString(result.getValue(CF.getBytes, "SERVICE_NAME".getBytes))));
    sDoc.addField("ADDRESS", etl_field(Bytes.toString(result.getValue(CF.getBytes, "ADDRESS".getBytes))));
    sDoc.addField("POSTAL_CODE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "POSTAL_CODE".getBytes))));
    sDoc.addField("PRINCIPAL", etl_field(Bytes.toString(result.getValue(CF.getBytes, "PRINCIPAL".getBytes))));
    sDoc.addField("PRINCIPAL_TEL", etl_field(Bytes.toString(result.getValue(CF.getBytes, "PRINCIPAL_TEL".getBytes))));
    sDoc.addField("INFOR_MAN", etl_field(Bytes.toString(result.getValue(CF.getBytes, "INFOR_MAN".getBytes))));
    sDoc.addField("INFOR_MAN_TEL", etl_field(Bytes.toString(result.getValue(CF.getBytes, "INFOR_MAN_TEL".getBytes))));
    sDoc.addField("INFOR_MAN_EMAIL", etl_field(Bytes.toString(result.getValue(CF.getBytes, "INFOR_MAN_EMAIL".getBytes))));
    sDoc.addField("ISP", etl_field(Bytes.toString(result.getValue(CF.getBytes, "ISP".getBytes))));
    sDoc.addField("STATUS", etl_field(Bytes.toString(result.getValue(CF.getBytes, "STATUS".getBytes))));
    sDoc.addField("ENDING_NUMS", etl_field(Bytes.toString(result.getValue(CF.getBytes, "ENDING_NUMS".getBytes))));
    sDoc.addField("SERVER_NUMS", etl_field(Bytes.toString(result.getValue(CF.getBytes, "SERVER_NUMS".getBytes))));
    sDoc.addField("SERVICE_IP", etl_field(Bytes.toString(result.getValue(CF.getBytes, "SERVICE_IP".getBytes))));
    sDoc.addField("NET_TYPE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "NET_TYPE".getBytes))));
    sDoc.addField("PRACTITIONER_COUNT", etl_field(Bytes.toString(result.getValue(CF.getBytes, "PRACTITIONER_COUNT".getBytes))));
    sDoc.addField("NET_MONITOR_DEPARTMENT", etl_field(Bytes.toString(result.getValue(CF.getBytes, "NET_MONITOR_DEPARTMENT".getBytes))));
    sDoc.addField("NET_MONITOR_MAN", etl_field(Bytes.toString(result.getValue(CF.getBytes, "NET_MONITOR_MAN".getBytes))));
    sDoc.addField("NET_MONITOR_MAN_TEL", etl_field(Bytes.toString(result.getValue(CF.getBytes, "NET_MONITOR_MAN_TEL".getBytes))));
    sDoc.addField("REMARK", etl_field(Bytes.toString(result.getValue(CF.getBytes, "REMARK".getBytes))));
    sDoc.addField("DATA_SOURCE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "DATA_SOURCE".getBytes))));
    sDoc.addField("MACHINE_ID", etl_field(Bytes.toString(result.getValue(CF.getBytes, "MACHINE_ID".getBytes))));
    sDoc.addField("SERVICE_NAME_PIN_YIN", etl_field(Bytes.toString(result.getValue(CF.getBytes, "SERVICE_NAME_PIN_YIN".getBytes))));
    sDoc.addField("ONLINE_STATUS", etl_field(Bytes.toString(result.getValue(CF.getBytes, "ONLINE_STATUS".getBytes))));
    sDoc.addField("UPDATE_TIME", etl_field(Bytes.toString(result.getValue(CF.getBytes, "UPDATE_TIME".getBytes))));
    sDoc.addField("PROBE_VERSION", etl_field(Bytes.toString(result.getValue(CF.getBytes, "PROBE_VERSION".getBytes))));
    sDoc.addField("TEMPLET_VERSION", etl_field(Bytes.toString(result.getValue(CF.getBytes, "TEMPLET_VERSION".getBytes))));
    sDoc.addField("LONGITUDE_LATITUDE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "LONGITUDE_LATITUDE".getBytes))));
    sDoc.addField("SPACE_SIZE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "SPACE_SIZE".getBytes))));
    sDoc.addField("LATITUDE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "LATITUDE".getBytes))));
    sDoc.addField("IP_ADDRESS", etl_field(Bytes.toString(result.getValue(CF.getBytes, "IP_ADDRESS".getBytes))));
    sDoc.addField("IS_OUTLINE_ALERT", etl_field(Bytes.toString(result.getValue(CF.getBytes, "IS_OUTLINE_ALERT".getBytes))));
    sDoc.addField("ELEVATION", etl_field(Bytes.toString(result.getValue(CF.getBytes, "ELEVATION".getBytes))));
    sDoc.addField("SERVICE_IMG", etl_field(Bytes.toString(result.getValue(CF.getBytes, "SERVICE_IMG".getBytes))));
    sDoc.addField("AGEN_LAVE_TIME", etl_field(Bytes.toString(result.getValue(CF.getBytes, "AGEN_LAVE_TIME".getBytes))));
    sDoc.addField("REAL_ENDING_NUMS", etl_field(Bytes.toString(result.getValue(CF.getBytes, "REAL_ENDING_NUMS".getBytes))));
    sDoc.addField("CAUSE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "CAUSE".getBytes))));
    sDoc.addField("IS_ALLOW_INSERT", etl_field(Bytes.toString(result.getValue(CF.getBytes, "IS_ALLOW_INSERT".getBytes))));
    sDoc.addField("BUSINESS_NATURE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "BUSINESS_NATURE".getBytes))));
    sDoc.addField("LAW_PRINCIPAL_CERTIFICATE_TYPE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "LAW_PRINCIPAL_CERTIFICATE_TYPE".getBytes))));
    sDoc.addField("LAW_PRINCIPAL_CERTIFICATE_ID", etl_field(Bytes.toString(result.getValue(CF.getBytes, "LAW_PRINCIPAL_CERTIFICATE_ID".getBytes))));
    sDoc.addField("START_TIME", etl_field(Bytes.toString(result.getValue(CF.getBytes, "START_TIME".getBytes))));
    sDoc.addField("END_TIME", etl_field(Bytes.toString(result.getValue(CF.getBytes, "END_TIME".getBytes))));
    sDoc.addField("IS_READ", etl_field(Bytes.toString(result.getValue(CF.getBytes, "IS_READ".getBytes))));
    sDoc.addField("ZIPNAME", etl_field(Bytes.toString(result.getValue(CF.getBytes, "ZIPNAME".getBytes))));
    sDoc.addField("BCPNAME", etl_field(Bytes.toString(result.getValue(CF.getBytes, "BCPNAME".getBytes))));
    sDoc.addField("ROWNUMBER", etl_field(Bytes.toString(result.getValue(CF.getBytes, "ROWNUMBER".getBytes))));
    sDoc.addField("INSTALL_TIME", etl_field(Bytes.toString(result.getValue(CF.getBytes, "INSTALL_TIME".getBytes))));
    sDoc.addField("POLIC_STATION", etl_field(Bytes.toString(result.getValue(CF.getBytes, "POLIC_STATION".getBytes))));
    sDoc.addField("MANUFACTURER_CODE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "MANUFACTURER_CODE".getBytes))));
    sDoc.addField("AREA_CODE", etl_field(Bytes.toString(result.getValue(CF.getBytes, "AREA_CODE".getBytes))));
  }

  def deleteSolrByQuery(query: String): Unit = {
    solrClient.deleteByQuery(query);
    solrClient.commit()
    println("数据已清空!")
  }

  //处理空数据
  def etl_field(field: String): String = {
    if (StringUtils.isBlank(field)) {
      return "";
    } else field
  }

}
