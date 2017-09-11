package main.scala.ML

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.PCA
import org.apache.spark.mllib.linalg.{SparseVector, VectorUDT, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by meng on 16-11-23.
  */
object ThiefDetection {


  case class whole_data(ID: String, CAPTURE_TIME: String, CERTIFICATE_CODE: String, CERTIFICATE_TYPE: String, DIST: String,
                        ENDING_MAC: String, EQUIPMENT_ID: String, FIELD_STRENGTH: String, IMPORT_TIME: String, SERVICE_CODE: String)

  def check_field(field: String): String = {
    if (StringUtils.isNotBlank(field)) {
      return field;
    }
    return "";
  }

  def main(Args: Array[String]): Unit = {
    //H_SCAN_ENDING_TRACE
    //System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.6.2")


    //Hidden the logs that are not needed
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //Setting up the running environment
    val sconf = new SparkConf().setAppName("RobinProject").setMaster("local[4]")
    //val sconf = new SparkConf().setAppName("RobinProject")

    val sc = new SparkContext(sconf)

    val hconf = HBaseConfiguration.create()

    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    //设置zookeeper集群
    hconf.set("hbase.zookeeper.quorum", "nn1,nn2,dn1,dn2,dn3")
    //设置HMatser
    hconf.set("hbase.zookeeper.master", "dn3:60000")

    hconf.set(TableInputFormat.INPUT_TABLE, "H_SCAN_ENDING_TRACE")


    //val sqlContext = new SQLContext(sc)

    //    Scan操作
    val hBaseRDD = sc.newAPIHadoopRDD(hconf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val ending_trace = hBaseRDD.map(r =>
      whole_data(
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
    )

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val dataSchema = sqlContext.createDataFrame(ending_trace)
    dataSchema.registerTempTable("whole_data")

    println("The running environment has been set up")

    //Read data from databases
    /*val wholeData = sqlContext.read.format("jdbc").options(
      Map(
        "url" -> "jdbc:mysql://localhost:3306/dbs_robin_project",
        "user" -> "root",
        "password" -> "5431333",
        "dbtable" -> "scan_ending_trace"
      )
    ).load().repartition(4)*/

    println("Data have been read from the database")

    //Obtain the original table：ending_mac, day_time, hour, equipment_id, num_appears

    //wholeData.registerTempTable("whole_data")

    val originalStats = sqlContext.sql(
      """
        |select ENDING_MAC, day(CAPTURE_TIME) as DAY_TIME, hour(CAPTURE_TIME) as HOUR_, EQUIPMENT_ID, count(ID) as NUM_APPEARS
        |from whole_data
        |group by day(CAPTURE_TIME), hour(CAPTURE_TIME), ENDING_MAC, EQUIPMENT_ID
        |order by ENDING_MAC asc, DAY_TIME asc, HOUR_ asc
      """.stripMargin)

    println("Original statistics have been extracted")

    //Convert the original table into wide table
    originalStats.registerTempTable("original_stats")
    sqlContext.udf.register("mergeTwoCols", mergeTwoCols(_: String, _: String))
    val tempStats = sqlContext.sql(
      """
        |select ENDING_MAC, DAY_TIME, mergeTwoCols(HOUR_, EQUIPMENT_ID) as merged_col, NUM_APPEARS
        |from original_stats
      """.stripMargin)

    val diffPivotValue = tempStats.select("merged_col")
      .distinct()
      .collect()
      .map(f => f.toString().filterNot(p => "[ ]".contains(p)))

    val reduceByEndingMac = (a: (String, String, Array[Double]), b: (String, String, Array[Double])) => {
      val lengthOfA = a._3.length
      val addOfAB = (for (i <- 0 until lengthOfA) yield {
        a._3(i) + b._3(i)
      }).toArray
      (a._1, a._2, addOfAB)
    }
    val wideStats = tempStats.map {
      f =>
        val seqedLine = f.toSeq
        (seqedLine.head.toString, (seqedLine.head.toString, seqedLine(1).toString, generateVariablesUnit(seqedLine(2).toString, seqedLine(3).toString.toInt, diffPivotValue)))
    }.reduceByKey(reduceByEndingMac).map {
      f =>
        val length = f._2._3.length
        val indices = (for (i <- 0 until length) yield i).toArray
        Row(f._2._1.toString, f._2._2.toString, Vectors.sparse(length, indices, f._2._3))
    }

    //Merge all feature columns in wide table into one column named features
    val mySchema = StructType(
      Array(StructField("ending_mac", StringType, nullable = true), StructField("date_time", StringType, nullable = true), StructField("features", new VectorUDT(), nullable = true))
    )
    val mergedDF = sqlContext.createDataFrame(wideStats, mySchema)
    println("All feature columns in wide table have been merged into one single column named 'features' with the merged table being cached")

    mergedDF.show(200)

    //Perform PCA on merged data frame to reduce the dimension of features
    val pcaModel = new PCA()
      .setK(24)
      .setInputCol("features")
      .setOutputCol("pca_features")
      .fit(mergedDF)
    val pcaDF = pcaModel.transform(mergedDF)
    pcaDF.cache()

    pcaDF.show(200)

    //Set up a data frame to store the clustering results for different K
    var clusterResults = pcaDF.select("ending_mac", "date_time")
    //Construct an array containing the K numbers by which a set of k-means clustering could be performed
    val totalNumRecords = mergedDF.count()

    for (k <- 1 until math.log10(totalNumRecords).toInt by 1) {
      //Perform the kmeans clustering
      println("Performing the kmeans clustering for k =" + math.pow(10, k).toInt)
      val kmeans = new KMeans()
        .setK(math.pow(10, k).toInt)
        .setFeaturesCol("pca_features")
        .setPredictionCol("k_" + math.pow(10, k).toInt)
      val model = kmeans.fit(pcaDF)
      val transformedDF = model.transform(pcaDF)
      clusterResults = clusterResults.join(transformedDF.select("ending_mac", "k_" + math.pow(10, k).toInt), "ending_mac")
    }

    /*val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "mysql")
    clusterResults.write.jdbc("jdbc:mysql://192.168.60.56:3306/test", "cluster_result", properties)*/

    /*clusterResults.rdd.foreachPartition {
      x => {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "nn1,nn2,dn1,dn2,dn3") //修改成自己的Hbase的配置
        myConf.set("hbase.zookeeper.property.clientPort", "2181") //修改成自己的Hbase的配置
        hconf.set("hbase.zookeeper.master", "dn3:60000")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, TableName.valueOf("CLUSTER_RESULT"))
        myTable.setAutoFlush(false, false) //关键点1
        myTable.setWriteBufferSize(3 * 1024 * 1024) //关键点2
        x.foreach { y => {
          println(y(0) + ":::" + y(1) + ":::" + y(2) + ":::" + y(3) + ":::" + y(4) + ":::" + y(5))

          val p = new Put(Bytes.toBytes(y(0).toString))//rowkey

          p.addColumn(Bytes.toBytes("RESULT"), Bytes.toBytes("date_time"), Bytes.toBytes(y(1).toString))
          p.addColumn(Bytes.toBytes("RESULT"), Bytes.toBytes("k_10"), Bytes.toBytes(y(2).toString))
          p.addColumn(Bytes.toBytes("RESULT"), Bytes.toBytes("k_100"), Bytes.toBytes(y(3).toString))
          p.addColumn(Bytes.toBytes("RESULT"), Bytes.toBytes("k_1000"), Bytes.toBytes(y(4).toString))
          p.addColumn(Bytes.toBytes("RESULT"), Bytes.toBytes("k_10000"), Bytes.toBytes(y(5).toString))

          myTable.put(p)
          }
        }
        myTable.flushCommits() //关键点3
      }
    }*/

    clusterResults.show(200)
  }


  def mergeTwoCols(HOUR: String, EQUIPMENT_ID: String): String = {
    HOUR + "_" + EQUIPMENT_ID
  }

  def generateVariablesUnit(merged_col: String, num_appears: Int, diff_pivot_value: Array[String]): Array[Double] = {
    val values = for (keyword <- diff_pivot_value) yield {
      if (merged_col == keyword) num_appears else 0.0
    }
    val indices = (for (i <- diff_pivot_value.indices) yield i).toArray
    val res = new SparseVector(diff_pivot_value.length, indices, values).toSparse
    values
  }
}