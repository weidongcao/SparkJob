package solrJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/9/19.
 */
public class WeiBo {


    //    public class HBaseIndexer {
    public static final Logger LOG = LoggerFactory.getLogger(WeiBo.class);

    /**
     * Mapper实现
     */
    static class SolrIndexerMapper extends TableMapper<Text, Text> {

        public enum Counters {ROWS}


        // 用于计数器
        private SolrClient solr; // 只创建一个SolrServer实例
        private int commitSize;
        private final List<SolrInputDocument> inputDocs = new ArrayList<>();

        /**
         * Called once at the beginning of the task.
         */
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            solr = new HttpSolrClient(conf.get("solr.server"));

            commitSize = conf.getInt("solr.commit.size", 1000); // 一次性添加的文档数，写在配置文件中
        }

        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context)
                throws IOException {
            SolrInputDocument solrDoc = new SolrInputDocument();
            for (KeyValue kv : values.list()) {
                String fieldName = Bytes.toString(kv.getQualifier()); // 直接将列标识符名作为域名
                String fieldValue = Bytes.toString(kv.getValue());
                solrDoc.addField(fieldName, fieldValue);
            }

            inputDocs.add(solrDoc);

            if (inputDocs.size() >= commitSize) {
                try {
                    LOG.info("添加文档:Adding " + Integer.toString(inputDocs.size()) + " documents");
                    solr.add(inputDocs); // 索引文档
                } catch (final SolrServerException e) {
                    final IOException ioe = new IOException();
                    ioe.initCause(e);
                    throw ioe;
                }
                inputDocs.clear();
            }
            context.getCounter(Counters.ROWS).increment(1);
        }

        /**
         * Called once at the end of the task.
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {
            try {
                if (!inputDocs.isEmpty()) {
                    LOG.info("清空队列:Adding " + Integer.toString(inputDocs.size()) + " documents");
                    solr.add(inputDocs);
                    inputDocs.clear();
                }
            } catch (final SolrServerException e) {
                final IOException ioe = new IOException();
                ioe.initCause(e);
                throw ioe;
            }
        }
    }

    public static Job createSubmittableJob(Configuration conf, String tablename)
            throws IOException {
        Job job = new Job(conf, "SolrIndex_" + tablename);
        job.setJarByClass(WeiBo.class);

        Scan scan = new Scan();
        scan.setCacheBlocks(false); // scan的数据不放在缓存中，一次性的

        /* 需要建索引的数据 */
        scan.addColumn(Bytes.toBytes("baseinfo"), Bytes.toBytes("title"));
        scan.addColumn(Bytes.toBytes("baseinfo"), Bytes.toBytes("user"));
        scan.addColumn(Bytes.toBytes("baseinfo"), Bytes.toBytes("classlabel"));
        scan.addColumn(Bytes.toBytes("baseinfo"), Bytes.toBytes("summary"));

        job.setOutputFormatClass(NullOutputFormat.class);
        TableMapReduceUtil.initTableMapperJob(tablename, scan,
                SolrIndexerMapper.class, null, null, job); // 不需要输出，键、值类型为null
        job.setNumReduceTasks(0); // 无reduce任务
        return job;
    }

    private static void printUsage(String errorMessage) {
        System.err.println("ERROR: " + errorMessage);
        System.err.println("Usage: VideoIndexer <tablename>");
    }

    /**
     * Main entry point.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setInt("hbase.client.scanner.caching", 100);

        if (args.length < 1) {
            printUsage("Wrong number of parameters: " + args.length);
            System.exit(-1);
        }
        String tablename = args[0];

        Job job = createSubmittableJob(conf, tablename);
        if (job == null) {
            System.exit(-1);
        }
        job.waitForCompletion(true);
        Counter counter = job.getCounters().findCounter(SolrIndexerMapper.Counters.ROWS);
        LOG.info("Put " + counter.getValue() + " records to Solr!"); // 打印日志
    }

}

