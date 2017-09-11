package main.java;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Created by Administrator on 2016/8/12.
 */
public class SparkTest {

    public static void main(String[] args) {

        /*System.setProperty("hadoop.home.dir" , "D:\\Program Files\\Java\\hadoop-2.6.2" );

        //创建一个java版本的spark context
        SparkConf conf = new SparkConf().setAppName("sparktest").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        //读取我们需要的数据
        String inPut ="D://Program Files//Java//JetBrains/workspace//SparkAction//input//tableFields";
        String outPut ="D://Program Files//Java//JetBrains/workspace//SparkAction//input//out";
//        JavaRDD<String> textFile = sc.textFile("c://test//contentdb.txt");
        JavaRDD<String> textFile = sc.textFile(inPut);
        //D://Program Files//Java//JetBrains/workspace//SparkAction//input//tableFields
        //切割数据
        JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) {
                System.out.println(s);
                return Arrays.asList(s.split(" "));
            }
        });

        //转换为键值对并计数
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });

//        counts.saveAsTextFile("c://test//a.txt");

        counts.saveAsTextFile(outPut);
    */}
}
