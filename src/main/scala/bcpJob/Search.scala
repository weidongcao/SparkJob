package main.scala.bcpJob

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext


/**
  * Created by jxl on 2016/10/20.
  */
object Search {

  //Hbase信息
  val TABLE_NAME = "H_REG_CONTENT_SEARCH_TMP";
  val CF = "CONTENT_SEARCH";

  val INPUT = "/opt/bcp/search/*search_content.bcp";
  val DEL_INPUT = "/opt/bcp/search";

  def main(args: Array[String]) {
    //    val sconf = new SparkConf().setAppName("indexServiceInfo")
    //    val sc = new SparkContext(sconf)
    val sc = new SparkContext("local", "SparkOnHBase")

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //设置zookeeper集群
    conf.set("hbase.zookeeper.quorum", "nn1,nn2,dn1,dn2,dn3")
    //设置HMatser
    conf.set("hbase.zookeeper.master","dn3:60000")



    // ======Save RDD to HBase========
    // step 1: JobConf setup
    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME)


    // step 2: rdd mapping to table
    def convert(fields: String) = {

      val field = fields.split("""\|#\|""")

      //if(field !=null && field.length == 22 ){


      val uuid = UUID.randomUUID().toString.replace("-", "")
      //println(uuid + "------------------" + field(0) + "------------------" + field(1))



      val p = new Put(Bytes.toBytes(uuid))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("SERVICEID"), Bytes.toBytes(field(0)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("SESSIONID"), Bytes.toBytes(field(1)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("CERTIFICATE_TYPE"), Bytes.toBytes(field(2)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("CERTIFICATE_CODE"), Bytes.toBytes(field(3)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("USERNAME"), Bytes.toBytes(field(4)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("SERVICETYPE"), Bytes.toBytes(field(5)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("URL"), Bytes.toBytes(field(6)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("HOST"), Bytes.toBytes(field(7)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("REFURL"), Bytes.toBytes(field(8)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("REFHOST"), Bytes.toBytes(field(9)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("KEYWORD"), Bytes.toBytes(field(10)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("KEYWORD_CODE"), Bytes.toBytes(field(11)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("REMOTEIP"), Bytes.toBytes(field(12)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("REMOTEPORT"), Bytes.toBytes(field(13)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("COMPUTERIP"), Bytes.toBytes(field(14)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("COMPUTERPORT"), Bytes.toBytes(field(15)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("COMPUTERMAC"), Bytes.toBytes(field(16)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("CAPTIME"), Bytes.toBytes(field(17)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("ROOM_ID"), Bytes.toBytes(field(18)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("CHECKIN_ID"), Bytes.toBytes(field(19)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("MACHINE_ID"), Bytes.toBytes(field(20)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("DATA_SOURCE"), Bytes.toBytes(field(field.length - 1).replace("|$|", "")))

      //3101042100013b|#|0A0A62A444D4E0CCD0C9580F587D|#|414|#|N3321847|#|ER SHI<<YUAN|#|1259301|#|map.baidu.com/?qt=bkg_data&ie=utf-8&rn=20&wd=%E4%B8%9C%E6%98%8E%E8%B7%AF383%E5%BC%8426%E5%8F%B7304&c=289&l=18&xy=52840_14192,52840_14193,52840_14194,52841_14192,52841_14193,52841_14194,52842_14192,52842_14193,52842_14194&b=(13527204.694952393,3633324.421006775;13527564.694952393,3633785.421006775)&callback=window._mdiuqp3zhi|#|map.baidu.com|#|http://map.baidu.com/mobile/webapp/place/detail/qt=s&wd=%E4%B8%9C%E6%98%8E%E8%B7%AF383%E5%BC%8426%E5%8F%B7304/i=0&showall=1&pos=0&da_ref=listclk&da_qrtp=41&da_log=sampid%3A3_eno%3A206_adnum%3A0_sid%3A7472492840299561_from%3Awebappmap_exptype%3Aurl_query%3A%E4%B8%9C%E6%98%8E%E8%B7%AF383%E5%BC%8426%E5%8F%B7304_adids%3A_killnum%3A0_userids%3A&da_adquery=%E4%B8%9C%E6%98%8E%E8%B7%AF383%E5%BC%8426%E5%8F%B7304&da_adtitle=%E4%B8%9C%E6%98%8E%E8%B7%AF383%E5%BC%8426%E5%8F%B7304&detail_from=list&third_party=ws%26pos%3D0%26ad_page_logs%3D%26semStatType%3Dlist&vt=map|#||#|�|#|%E4%B8%9C%E6%98%8E%E8%B7%AF383%E5%BC%8426%E5%8F%B7304|#|2001460807|#|80|#|168452772|#|52290|#|44D4E0CCD0C9|#|2016-10-26 17:05:19|#|b1102|#|3101042100013bb1102x3218471477188868|#|NIH61C1201412QF005|#|1|$|

      (new ImmutableBytesWritable, p)

      ///}else


      //(new ImmutableBytesWritable, "")

    }

    try{
    val dateFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // step 3: read RDD data from somewhere and convert
    //    val readFile = sc.textFile("file://" + INPUT)
    val readFile = sc.textFile("file://" + INPUT)
    println("开始索引时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + dateFormater.format(new Date) + "索引总条目：<<<<<<<<<<<<<<<<<<<<<<<<" + readFile.count())
    val localData = readFile.filter(_.split("""\|#\|""").length == 22).map(convert)

    //step 4: use `saveAsHadoopDataset` to save RDD to HBase
    localData.saveAsHadoopDataset(jobConf)
    println("结束索引时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + dateFormater.format(new Date))
    } catch {
      case e: Exception => println(e.printStackTrace()); System.exit(-1)
      case unknown => println("Unknown exception " + unknown); System.exit(-1)
    } finally {
      //关闭SparkContext上下文
      sc.stop()
      //清空文件夹
      deleteFolder(DEL_INPUT)
    }

  }

  def deleteFolder(dir: String) {
    val delfolder = new File(dir);
    val oldFile = delfolder.listFiles();
    try {
      for (i <- 0 to oldFile.length - 1) {
        if (oldFile(i).isDirectory()) {
          deleteFolder(dir + "/" + oldFile(i).getName()); //递归清空子文件夹
        }
        oldFile(i).delete();
      }
      println("清空文件夹操作成功!")
    }
    catch {
      case e: Exception => println(e.printStackTrace());
        println("清空文件夹操作出错!")
        System.exit(-1)
    }
  }
}
