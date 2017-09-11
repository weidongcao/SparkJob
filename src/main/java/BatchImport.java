package main.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BatchImport {
    static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
    }

    static class BatchImportMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        SimpleDateFormat dateformat1 = new SimpleDateFormat("yyyyMMddHHmmss");
        Text v2 = new Text();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] splited = value.toString().split("|$|\\n");
            try {

                System.out.println(value.toString());
                final Date date = new Date(Long.parseLong(splited[0].trim()));
                final String dateFormat = dateformat1.format(date);
                String rowKey = splited[0] + ":" + dateFormat;
                v2.set(rowKey + "_" + value.toString());
                context.write(key, v2);
            } catch (NumberFormatException e) {
                final Counter counter = context.getCounter("BatchImport", "ErrorFormat");
                counter.increment(1L);
                System.out.println("" + splited[0] + " " + e.getMessage());
            }
        }

        ;
    }

    static class BatchImportReducer extends TableReducer<LongWritable, Text, NullWritable> {
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                System.out.println(text.toString());
                final String[] splited = text.toString().split("|#|");

//				final Put put = new Put(Bytes.toBytes(splited[0]));
//				put.add(Bytes.toBytes("CF"), Bytes.toBytes("SESSIONID"), Bytes.toBytes(splited[0]));
//				put.add(Bytes.toBytes("CF"), Bytes.toBytes("URL"), Bytes.toBytes(splited[1]));
//				put.add(Bytes.toBytes("CF"), Bytes.toBytes("NAME"), Bytes.toBytes(splited[2]));

//				context.write(NullWritable.get(), put);
            }
        }

        ;
    }

    public static void main(String[] args) throws Exception {


        //final Configuration configuration = new Configuration();

        conf.set("hbase.zookeeper.quorum", "nn1.hadoop.com");

        conf.set(TableOutputFormat.OUTPUT_TABLE, "BBS_TEST");

        conf.set("dfs.socket.timeout", "180000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        final Job job = new Job(conf, "BatchImport");

        job.setMapperClass(BatchImportMapper.class);
        job.setReducerClass(BatchImportReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(TableOutputFormat.class);

        FileInputFormat.setInputPaths(job, "input/1466645158-16186-im_chat.bcp");

        job.waitForCompletion(true);
        //job.submit();
    }


}