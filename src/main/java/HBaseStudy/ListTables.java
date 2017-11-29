package HBaseStudy;

/**
 * Created by jinyuzhang on 11/22/17.
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class ListTables {
    public static void main(String args[])throws MasterNotRunningException, IOException{

        // Instantiating a configuration class
        Configuration conf = HBaseConfiguration.create();
      //  conf.set("hbase.master", "test-hadoop1:600000");
      //  conf.set("hbase.cluster.distributed", true);

        // Instantiating HBaseAdmin class
        HBaseAdmin admin = new HBaseAdmin(conf);

        // Getting all the list of tables using HBaseAdmin object
        HTableDescriptor[] tableDescriptor =admin.listTables();

        // printing all the table names.
        for (int i=0; i<tableDescriptor.length;i++ ){
            System.out.println(tableDescriptor[i].getNameAsString());
        }

    }
}
