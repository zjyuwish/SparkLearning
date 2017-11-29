package HBaseStudy;

/**
 * Created by jinyuzhang on 11/27/17.
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.util.Tool;
public class WordCountUploadToHBase extends Configured {
    public static class WCHBaseMapper extends Mapper<Object, Text, ImmutableBytesWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer strs = new StringTokenizer(value.toString());
            while(strs.hasMoreTokens()) {
                word.set(strs.nextToken());
                context.write(new ImmutableBytesWritable(Bytes.toBytes(word.toString())), one);
            }
        }
    }
    public static class WCHBaseReducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
        public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val: values) {
                sum += val.get();
            }
            Put put = new Put(key.get());
            put.add(Bytes.toBytes("content"), Bytes.toBytes("count"), Bytes.toBytes(sum + ""));
            context.write(key, put);
        }
    }
    public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException,IOException, ClassNotFoundException, InterruptedException {
        String tableName = "wordcount";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "test-hadoop1:600000");
        conf.set("hbase.zookeeper.quorum", "test-hadoop1,test-hadoop3,test-hadoop4");

        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor =new HColumnDescriptor("content");
        tableDescriptor.addFamily(columnDescriptor);
        admin.createTable(tableDescriptor);

        Job job = new Job(conf,"upload to hbase");
        job.setJarByClass(WordCountUploadToHBase.class);
        job.setMapperClass(WCHBaseMapper.class);
        TableMapReduceUtil.initTableReducerJob(tableName, WCHBaseReducer.class, job,null,null,null,null,false);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        FileInputFormat.addInputPaths(job, "/test_zjy/hbase-test.txt");
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
