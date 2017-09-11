package App

import java.util.UUID

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
object SparkIndexBcp {

  //Hbase信息
  //  val TABLE_NAME = "BBS_TEST";
  //  val CF = "CF";
  val TABLE_NAME = "H_SERVICE_INFO";
  val CF = "SERVICE_INFO";

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.6.2")

    val sc = new SparkContext("local", "SparkOnHBase")

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "dn2.hadoop.com")


    // ======Save RDD to HBase========
    // step 1: JobConf setup
    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME)


    // step 2: rdd mapping to table

    // 在 HBase 中表的 schema 一般是这样的
    // *row   cf:col_1    cf:col_2
    // 而在Spark中，我们操作的是RDD元组，比如(1,"lilei",14) , (2,"hanmei",18)
    // 我们需要将 *RDD[(uid:Int, name:String, age:Int)]* 转换成 *RDD[(ImmutableBytesWritable, Put)]*
    // 我们定义了 convert 函数做这个转换工作
    /*def convert(triple: (Int, String, Int)) = {
      val p = new Put(Bytes.toBytes(triple._1))
      p.addColumn(Bytes.toBytes("SERVICE_INFO"), Bytes.toBytes("name"), Bytes.toBytes(triple._2))
      p.addColumn(Bytes.toBytes("SERVICE_INFO"), Bytes.toBytes("age"), Bytes.toBytes(triple._3))
      (new ImmutableBytesWritable, p)
    }*/

    def convert(fields: String) = {

      val field = fields.split("""\|#\|""")

      val uuid = UUID.randomUUID().toString.replace("-", "")
      println(uuid + "------------------" + field(0) + "------------------" + field(1))
      val p = new Put(Bytes.toBytes(uuid))

      //      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("SESSIONID"), Bytes.toBytes(field(0)))
      //      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("URL"), Bytes.toBytes(field(1)))
      //      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("NAME"), Bytes.toBytes(field(field.length - 1).replace("|$|", "")))

      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("SERVICEID"), Bytes.toBytes(field(0)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("SERVICE_NAME"), Bytes.toBytes(field(1)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("ADDRESS"), Bytes.toBytes(field(2)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("POSTAL_CODE"), Bytes.toBytes(field(3)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("PRINCIPAL"), Bytes.toBytes(field(4)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("PRINCIPAL_TEL"), Bytes.toBytes(field(5)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("INFOR_MAN"), Bytes.toBytes(field(6)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("INFOR_MAN_TEL"), Bytes.toBytes(field(7)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("INFOR_MAN_EMAIL"), Bytes.toBytes(field(8)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("ISP"), Bytes.toBytes(field(9)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("STATUS"), Bytes.toBytes(field(10)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("ENDING_COUNT"), Bytes.toBytes(field(11)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("SERVER_COUNT"), Bytes.toBytes(field(12)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("SERVICE_IP"), Bytes.toBytes(field(13)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("NET_TYPE"), Bytes.toBytes(field(14)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("PRACTITIONER_COUNT"), Bytes.toBytes(field(15)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("NET_MONITOR_DEPARTMENT"), Bytes.toBytes(field(16)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("NET_MONITOR_MAN"), Bytes.toBytes(field(17)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("NET_MONITOR_MAN_TEL"), Bytes.toBytes(field(18)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("REMARK"), Bytes.toBytes(field(19)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("PROBE_VERSION"), Bytes.toBytes(field(20)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("SERVICESTATUS"), Bytes.toBytes(field(21)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("COMPUTERNUMS"), Bytes.toBytes(field(22)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("SERVICE_TYPE"), Bytes.toBytes(field(23)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("MACHINE_ID"), Bytes.toBytes(field(24)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("DATA_SOURCE"), Bytes.toBytes(field(25)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("LONGITUDE"), Bytes.toBytes(field(26)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("LATITUDE"), Bytes.toBytes(field(27)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("NETSITE_TYPE"), Bytes.toBytes(field(28)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("BUSINESS_NATURE"), Bytes.toBytes(field(29)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("LAW_PRINCIPAL_CERTIFICATE_TYPE"), Bytes.toBytes(field(30)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("LAW_PRINCIPAL_CERTIFICATE_ID"), Bytes.toBytes(field(31)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("START_TIME"), Bytes.toBytes(field(32)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("END_TIME"), Bytes.toBytes(field(33)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("CODE_ALLOCATION_ORGANIZATION"), Bytes.toBytes(field(34)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("POLICE_STATION_CODE"), Bytes.toBytes(field(35)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("TEMPLET_VERSION"), Bytes.toBytes(field(36)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("SPACE_SIZE"), Bytes.toBytes(field(37)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("IP_ADDRESS"), Bytes.toBytes(field(38)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("IS_OUTLINE_ALERT"), Bytes.toBytes(field(39)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("ELEVATION"), Bytes.toBytes(field(40)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("SERVICE_IMG"), Bytes.toBytes(field(41)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("AGEN_LAVE_TIME"), Bytes.toBytes(field(42)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("REAL_ENDING_NUMS"), Bytes.toBytes(field(43)))
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("CAUSE"), Bytes.toBytes(field(field.length - 1).replace("|$|", "")))


      (new ImmutableBytesWritable, p)
    }

    // step 3: read RDD data from somewhere and convert
    //val rawData = List((1, "lilei", 14), (2, "hanmei", 18), (3, "someone", 38))
    val readFile = sc.textFile("input/*service_info.bcp")
    //val readFile = sc.textFile("input/1466645280-16218-service_info.bcp").map(x => x.split("""\|#\|"""))

    //val localData = readFile.map(convert)
    val localData = readFile.map(convert)
    //    val localData = sc.parallelize(readFile).map(convert)
    //localData.foreach(_._2.get)

    //step 4: use `saveAsHadoopDataset` to save RDD to HBase
    localData.saveAsHadoopDataset(jobConf)

    jobConf.clear()
    sc.stop()
    // =================================


    /*// ======Load RDD from HBase========
    // use `newAPIHadoopRDD` to load RDD from HBase
    //直接从 HBase 中读取数据并转成 Spark 能直接操作的 RDD[K,V]

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "user")

    //添加过滤条件，年龄大于 18 岁
    val scan = new Scan()
    scan.setFilter(new SingleColumnValueFilter("basic".getBytes, "age".getBytes,
      CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(18)))
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val usersRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = usersRDD.count()
    println("Users RDD Count:" + count)
    usersRDD.cache()

    //遍历输出
    usersRDD.foreach { case (_, result) =>
      val key = Bytes.toInt(result.getRow)
      val name = Bytes.toString(result.getValue("basic".getBytes, "name".getBytes))
      val age = Bytes.toInt(result.getValue("basic".getBytes, "age".getBytes))
      println("Row key:" + key + " Name:" + name + " Age:" + age)
    }
    // =================================*/
  }
}
