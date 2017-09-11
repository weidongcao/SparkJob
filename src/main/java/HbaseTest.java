package main.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by Administrator on 2016/8/11.
 */
public class HbaseTest implements Serializable {
    public static HTable getTable(String tableName) throws IOException {
        // 创建HBase的配置对象
        Configuration conf = HBaseConfiguration.create();

        // 获取HBase的Table
        HTable table = new HTable(conf, "H_SERVICE_INFO");

        return table;

    }


    /**
     * 获取HBase Table数据
     *
     * @throws IOException
     */
    public static void getData() throws IOException {
    // 获取HBase Table
        HTable table = getTable("H_SERVICE_INFO");

    // 获取RowKey
        Get get = new Get(Bytes.toBytes("83540c57b01248779b23ae055a4182df"));
    // 根据列名称获取指定列
        // get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name")) ;

        Result result = table.get(get);
        Cell[] cells = result.rawCells();

        for (Cell cell : cells) {
            System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + ":");
            System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell))
                    + "-->");
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }

        table.close();
    }



    public static void main(String[] args) throws IOException {
    System.setProperty("hadoop.home.dir" , "D:\\Program Files\\Java\\hadoop-2.6.2" );
        getData();
    }
}
