package main.scala.ML

import java.util.Properties

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.PCA
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by meng on 16-11-23.
  */
object ThiefDetectionBeta extends Serializable {

  //Hbase信息
  val INPUT_TABLE_NAME = "H_SCAN_ENDING_TRACE";
  val OUTPUT_TABLE_NAME = "H_CLUSTER_RESULT";
  //列族
  val INPUT_CF = "SCAN_ENDING_TRACE";
  val OUTPUT_CF = "CLUSTER_RESULT";

  case class WHOLE_DATA(ID: String, CAPTURE_TIME: String, CERTIFICATE_CODE: String, CERTIFICATE_TYPE: String, DIST: String,
                        ENDING_MAC: String, EQUIPMENT_ID: String, FIELD_STRENGTH: String, IMPORT_TIME: String, SERVICE_CODE: String)


  def main(Args: Array[String]): Unit = {

    //Hidden the logs that are not needed
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //Setting up the running environment
    val conf = new SparkConf().setAppName("Kmeans").setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    println("--------------------------The running environment has been set up")

    //Read data from databases main.scala.ML.ThiefDetectionBeta


    /*val wholeData = sqlContext.read.format("jdbc").options(
      Map(
        "url" -> "jdbc:mysql://localhost:3306/dbs_robin_project",
        "user" -> "root",
        "password" -> "5431333",
        "dbtable" -> "scan_ending_trace"
      )
    ).load().repartition(4)*/

    val hconf = HBaseConfiguration.create()

    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    //设置zookeeper集群
    hconf.set("hbase.zookeeper.quorum", "node1,node2,data1,data2,data3")
    //设置HMatser
    hconf.set("hbase.zookeeper.master", "data3:60000")

    hconf.set(TableInputFormat.INPUT_TABLE, INPUT_TABLE_NAME)


    //val sqlContext = new SQLContext(sc)

    //    Scan操作
    val hBaseRDD = sc.newAPIHadoopRDD(hconf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val whole_Data = hBaseRDD.map(r =>
      WHOLE_DATA(
        Bytes.toString(r._2.getRow),
        check_field(Bytes.toString(r._2.getValue(INPUT_CF.getBytes, "CAPTURE_TIME".getBytes))),
        check_field(Bytes.toString(r._2.getValue(INPUT_CF.getBytes, "CERTIFICATE_CODE".getBytes))),
        check_field(Bytes.toString(r._2.getValue(INPUT_CF.getBytes, "CERTIFICATE_TYPE".getBytes))),
        check_field(Bytes.toString(r._2.getValue(INPUT_CF.getBytes, "DIST".getBytes))),
        check_field(Bytes.toString(r._2.getValue(INPUT_CF.getBytes, "ENDING_MAC".getBytes))),
        check_field(Bytes.toString(r._2.getValue(INPUT_CF.getBytes, "EQUIPMENT_ID".getBytes))),
        check_field(Bytes.toString(r._2.getValue(INPUT_CF.getBytes, "FIELD_STRENGTH".getBytes))),
        check_field(Bytes.toString(r._2.getValue(INPUT_CF.getBytes, "IMPORT_TIME".getBytes))),
        check_field(Bytes.toString(r._2.getValue(INPUT_CF.getBytes, "SERVICE_CODE".getBytes)))
      )
    )

    val wholeData = sqlContext.createDataFrame(whole_Data)



    println("--------------------------Data have been read from the database counts is------" + hBaseRDD.count())

    //Obtain the original table：ending_mac, day_time, hour, equipment_id, num_appears
    wholeData.registerTempTable("whole_data")

    val originalStats = sqlContext.sql(
      """
        |select ENDING_MAC, year(CAPTURE_TIME) as YEAR_TIME, month(CAPTURE_TIME) as MONTH_TIME, day(CAPTURE_TIME) as DAY_TIME, hour(CAPTURE_TIME) as HOUR_, minute(CAPTURE_TIME) as MINUTE_, EQUIPMENT_ID, count(ID) as NUM_APPEARS
        |from whole_data
        |group by year(CAPTURE_TIME), month(CAPTURE_TIME), day(CAPTURE_TIME), hour(CAPTURE_TIME), minute(CAPTURE_TIME), ENDING_MAC, EQUIPMENT_ID
        |order by ENDING_MAC asc, DAY_TIME asc, HOUR_ asc
      """.stripMargin)

    println("--------------------------Original statistics have been extracted")

    //Convert the original table into wide table
    originalStats.registerTempTable("original_stats")
    sqlContext.udf.register("mergeTwoCols", mergeThreeCols(_: String, _: String, _: String))
    val tempStats = sqlContext.sql(
      """
        |select ENDING_MAC, YEAR_TIME, MONTH_TIME, DAY_TIME, mergeTwoCols(HOUR_, MINUTE_, EQUIPMENT_ID) as merged_col, NUM_APPEARS
        |from original_stats
      """.stripMargin)
    val diffPivotValue = tempStats.select("merged_col")
      .distinct()
      .collect()
      .map(f => f.toString().filterNot(p => "[ ]".contains(p)))
    val reduceByEndingMac = (a: (String, String, String, String, Array[Double]), b: (String, String, String, String, Array[Double])) => {
      val lengthOfA = a._5.length
      val addOfAB = (for (i <- 0 until lengthOfA) yield {
        a._5(i) + b._5(i)
      }).toArray
      (a._1, a._2, a._3, a._4, addOfAB)
    }

    val wideStats = tempStats.map {
      f =>
        val seqedLine = f.toSeq
        ((seqedLine.head.toString, seqedLine(1).toString, seqedLine(2).toString, seqedLine(3).toString), (seqedLine.head.toString, seqedLine(1).toString, seqedLine(2).toString, seqedLine(3).toString, generateVariablesUnit(seqedLine(4).toString, seqedLine(5).toString.toInt, diffPivotValue)))
    }.reduceByKey(reduceByEndingMac).map {
      f =>
        val length = f._2._5.length
        val indices = (for (i <- 0 until length) yield i).toArray
        Row(f._2._1.toString, f._2._2.toString, f._2._3.toString, f._2._4.toString, Vectors.sparse(length, indices, f._2._5))
    }

    //Merge all feature columns in wide table into one column named features
    val mySchema = StructType(
      Array(StructField("ending_mac", StringType, nullable = true), StructField("year_time", StringType, nullable = true), StructField("month_time", StringType, nullable = true), StructField("date_time", StringType, nullable = true), StructField("features", new VectorUDT(), nullable = true))
    )
    val mergedDF = sqlContext.createDataFrame(wideStats, mySchema)

    println("--------------------------All feature columns in wide table have been merged into one single column named 'features' with the merged table being cached")

    //Perform PCA on merged data frame to reduce the dimension of features
    val pcaModel = new PCA()
      .setK(24)
      .setInputCol("features")
      .setOutputCol("pca_features")
      .fit(mergedDF)
    val pcaDF = pcaModel.transform(mergedDF)
    pcaDF.cache()

    //Set up a data frame to store the clustering results for different K
    var clusterResults = pcaDF.select("ending_mac", "date_time")
    //Construct an array containing the K numbers by which a set of k-means clustering could be performed
    //val totalNumRecords = pcaDF.count()
    for (k <- Array(10, 100, 1000)) {
      //Perform the kmeans clustering

      println("--------------------------Performing the kmeans clustering for k =" + k)

      val kmeans = new KMeans()
        .setK(k)
        .setFeaturesCol("pca_features")
        .setPredictionCol("k_" + k)
      val model = kmeans.fit(pcaDF)
      val transformedDF = model.transform(pcaDF)
      clusterResults = clusterResults.join(transformedDF.select("ending_mac", "k_" + k), "ending_mac")
    }

    /*val properties = new Properties()
    properties.put("user", "yunan")
    properties.put("password", "yunan")
    clusterResults.write.jdbc("jdbc:oracle:thin:@172.16.100.18:1521:orcl", "cluster_result", properties)*/




    clusterResults.cache()
    val resultRdd = clusterResults.rdd

    //write hdfs
    //resultRdd.saveAsTextFile("hdfs://dn1:8020/Kmeans")

    /*val oracleDriverUrl = "jdbc:oracle:thin:@172.16.100.18:1521:orcl"

    //overwrite JdbcDialect fitting for Oracle
    val OracleDialect = new JdbcDialect {

      override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle") || url.contains("oracle")

      //getJDBCType is used when writing to a JDBC table
      override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
        case StringType => Some(JdbcType("VARCHAR2(255)", java.sql.Types.VARCHAR))
        case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.NUMERIC))
        case IntegerType => Some(JdbcType("NUMBER(16)", java.sql.Types.NUMERIC))
        case LongType => Some(JdbcType("NUMBER(16)", java.sql.Types.NUMERIC))
        case DoubleType => Some(JdbcType("NUMBER(16,4)", java.sql.Types.NUMERIC))
        case FloatType => Some(JdbcType("NUMBER(16,4)", java.sql.Types.NUMERIC))
        case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.NUMERIC))
        case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.NUMERIC))
        case BinaryType => Some(JdbcType("BLOB", java.sql.Types.BLOB))
        case TimestampType => Some(JdbcType("DATE", java.sql.Types.DATE))
        case DateType => Some(JdbcType("DATE", java.sql.Types.DATE))
        case DecimalType.Unlimited => Some(JdbcType("NUMBER(38,4)", java.sql.Types.NUMERIC))
        case _ => None
      }
    }


    JdbcDialects.registerDialect(OracleDialect)

    val connectProperties = new Properties()
    connectProperties.put("user", "yunan")
    connectProperties.put("password", "yunan")
    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

    //write back Oracle
    //Note: When writing the results back orale, be sure that the target table existing
    JdbcUtils.saveTable(clusterResults, oracleDriverUrl, "cluster_results", connectProperties)
*/





    // ======Save RDD to HBase========
    // step 1: JobConf setup
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "node1,node2,data1,data2,data3")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set("hbase.zookeeper.master", "data3:60000")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, OUTPUT_TABLE_NAME)

    val job = Job.getInstance(sc.hadoopConfiguration) //new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])






    /*val jobConf = new JobConf(hconf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, OUTPUT_TABLE_NAME)*/


    //val resultData = clusterResults.rdd.take(100).foreach(println)   //map(convert)

    val resultData = resultRdd.map(convert)

    //resultData.saveAsHadoopDataset(jobConf)
    resultData.saveAsNewAPIHadoopDataset(job.getConfiguration())

  }

  // step 2: rdd mapping to table
  def convert(row: Row) = {

    println(row.get(0) + "-----" + row.get(1) + "-----" + row.get(2) + "-----" + row.get(3) + "-----" + row.get(4))

    if (row.length == 5) {
      val p = new Put(Bytes.toBytes(new String(row.get(0).toString.getBytes, "UTF-8")))
      //p.addColumn(Bytes.toBytes(OUTPUT_CF), Bytes.toBytes("DATE_TIME"), Bytes.toBytes(row.get(1).toString))
      p.addColumn(Bytes.toBytes(OUTPUT_CF), Bytes.toBytes("DATE_TIME"), Bytes.toBytes(new String(row.get(1).toString.getBytes, "UTF-8")))
      p.addColumn(Bytes.toBytes(OUTPUT_CF), Bytes.toBytes("K_10"), Bytes.toBytes(new String(row.get(2).toString.getBytes, "UTF-8")))
      p.addColumn(Bytes.toBytes(OUTPUT_CF), Bytes.toBytes("K_100"), Bytes.toBytes(new String(row.get(3).toString.getBytes, "UTF-8")))
      p.addColumn(Bytes.toBytes(OUTPUT_CF), Bytes.toBytes("K_1000"), Bytes.toBytes(new String(row.get(4).toString.getBytes, "UTF-8")))
      (new ImmutableBytesWritable, p)
    } else {
      val p = new Put(Bytes.toBytes(""))
      p.addColumn(Bytes.toBytes(OUTPUT_CF), Bytes.toBytes("DATE_TIME"), Bytes.toBytes(""))
      p.addColumn(Bytes.toBytes(OUTPUT_CF), Bytes.toBytes("K_10"), Bytes.toBytes(""))
      p.addColumn(Bytes.toBytes(OUTPUT_CF), Bytes.toBytes("K_100"), Bytes.toBytes(""))
      p.addColumn(Bytes.toBytes(OUTPUT_CF), Bytes.toBytes("K_1000"), Bytes.toBytes(""))
      (new ImmutableBytesWritable, p)
    }

  }

  def check_field(field: String): String = {
    if (StringUtils.isNotBlank(field)) {
      return field;
    }
    return "";
  }

  def mergeThreeCols(HOUR: String, MINUTE: String, EQUIPMENT_ID: String): String = {
    HOUR + "_" + MINUTE + "_" + EQUIPMENT_ID
  }

  def generateVariablesUnit(merged_col: String, num_appears: Int, diff_pivot_value: Array[String]): Array[Double] = {
    val values = for (keyword <- diff_pivot_value) yield {
      if (merged_col == keyword) num_appears else 0.0
    }
    //val indices = (for (i <- diff_pivot_value.indices) yield i).toArray
    //val res = new SparseVector(diff_pivot_value.length, indices, values).toSparse
    return values;
  }
}
