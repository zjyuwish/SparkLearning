package HBaseStudy.SecondaryIndex;

/**
 * Created by jinyuzhang on 11/28/17.
 */
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;


public class IndexBuilder {
    private String rootDir;
    private String zkServer;
    private String port;
    private Configuration conf;
    private HConnection hConn = null;

    private IndexBuilder(String rootDir,String zkServer,String port) throws IOException{
        this.rootDir = rootDir;
        this.zkServer = zkServer;
        this.port = port;

        conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", rootDir);
        conf.set("hbase.zookeeper.quorum", zkServer);
        conf.set("hbase.zookeeper.property.clientPort", port);

        hConn = HConnectionManager.createConnection(conf);
    }

    static class MyMapper extends TableMapper<ImmutableBytesWritable, Put>{

        //记录了要进行索引的列
        private Map<byte[], ImmutableBytesWritable> indexes = new
                HashMap<byte[], ImmutableBytesWritable>();

        private String familyName;

        @Override
        protected void map(ImmutableBytesWritable key, Result value,
                           Context context) throws IOException, InterruptedException {
            //原始表列
            Set<byte[]> keys = indexes.keySet();

            //索引表的rowkey是原始表的列，索引表的列是原始表的rowkey

            for (byte[] k : keys){

                //获得新建索引表的表名
                ImmutableBytesWritable indexTableName = indexes.get(k);

                //Result存放的是原始表的数据
                //查找到内容             根据列族 和 列 得到原始表的值
                byte[] val = value.getValue(Bytes.toBytes(familyName), k);

                if (val != null) {
                    //索引表
                    Put put = new Put(val);//索引表行键
                    //列族  列   原始表的行键
                    put.add(Bytes.toBytes("f1"),Bytes.toBytes("id"),key.get());
                    context.write(indexTableName, put);
                }
            }

        }

        //真正运行Map之前执行一些处理。
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            //通过上下文得到配置
            Configuration conf = context.getConfiguration();

            //获得表名
            String tableName = conf.get("tableName");
            //String family = conf.get("familyName");
            //获得列族
            familyName = conf.get("columnFamily");

            //获得列
            String[] qualifiers = conf.getStrings("qualifiers");

            for (String qualifier : qualifiers) {
                //建立一个映射，为每一个列创建一个表，表的名字tableName+"-"+qualifier
                //原始表的列    索引表新建表名
                System.out.println("Hello World1");
                indexes.put(Bytes.toBytes(qualifier),
                        new ImmutableBytesWritable(Bytes.toBytes(tableName+"-"+qualifier)));
            }

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String rootDir = "hdfs://test-hadoop1:8020/hbase";
        String zkServer = "test-hadoop1,test-hadoop3,test-hadoop4";
        String port = "2181";

        IndexBuilder conn = new IndexBuilder(rootDir,zkServer,port);

        String[] otherArgs = new GenericOptionsParser(conn.conf, args).getRemainingArgs();


        //IndexBuilder: TableName,ColumnFamily,Qualifier
        if(otherArgs.length<3){
            System.exit(-1);
        }
        //表名
        String tableName = otherArgs[0];
        //列族
        String columnFamily = otherArgs[1];

        conn.conf.set("tableName", tableName);
        conn.conf.set("columnFamily", columnFamily);

        //列  可能存在多个列
        String[] qualifiers = new String[otherArgs.length-2];

        for (int i = 0; i < qualifiers.length; i++) {
            qualifiers[i] = otherArgs[i+2];
        }

        //设置列
        conn.conf.setStrings("qualifiers", qualifiers);

        @SuppressWarnings("deprecation")
        Job job = new Job(conn.conf,tableName);

        job.setJarByClass(IndexBuilder.class);

        job.setMapperClass(MyMapper.class);
        job.setNumReduceTasks(0);//由于不需要执行reduce阶段

        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(MultiTableOutputFormat.class);

        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(tableName,scan,
                MyMapper.class, ImmutableBytesWritable.class, Put.class, job);

        job.waitForCompletion(true);

    }
}