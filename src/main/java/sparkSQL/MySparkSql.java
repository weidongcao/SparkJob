package main.java.sparkSQL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.Tuple2;

import java.io.Serializable;
import java.util.logging.Logger;

/**
 * Created by jxl on 2016/11/22.
 */

public class MySparkSql implements Serializable {

    Logger logger = Logger.getLogger(MySparkSql.class.getName());

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.6.2");

        SparkConf sconf = new SparkConf()
                .setMaster("local")
                .setAppName("MySparkSql") //win7环境下设置
                .set("spark.executor.memory", "1g")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkContext sc = new SparkContext(sconf);

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "dn2.hadoop.com");
        conf.set(TableInputFormat.INPUT_TABLE, "BBS_TEST");

        RDD<Tuple2<ImmutableBytesWritable, Result>> hBaseRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class);


        /*val bbs = hBaseRDD.map(m = > m._2.listCells()).map(c = >
                    Bbs(new String(c.get(0).getRow()),
                        new String(c.get(0).getValue),
                        new String(c.get(1).getValue),
                        new String(c.get(2).getValue))
                )*/


    }


}
