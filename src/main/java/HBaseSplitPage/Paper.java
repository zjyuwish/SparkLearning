package HBaseSplitPage;

/**
 * Created by jinyuzhang on 11/22/17.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.json.JSONObject;
import org.json.JSONArray;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;


public class Paper {
    public static Configuration configuration;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", "test-hadoop1:600000");
        configuration.set("hbase.zookeeper.quorum", "test-hadoop1,test-hadoop3,test-hadoop4");
    }

    private String tableName;
    private static HTable hTable;
    private static String startRow = null;
    private static List list = null;

    public Paper(String tableName) {
        try {
            this.hTable = new HTable(configuration, tableName.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List getLast (int pageNum, int pageSize) {
        getPage(pageNum - 1, pageSize);
        return null;
    }

    /**
     * 取得下一页 这个类是接着getPage来用
     * @param pageSize 分页的大小
     * @return  返回分页数据
     */

    public static List getNext(int pageSize) throws Exception {
        Filter filter = new PageFilter(pageSize + 1);
        Scan scan = new Scan();
        scan.setFilter(filter);
        scan.setStartRow(startRow.getBytes());
        ResultScanner result = hTable.getScanner(scan);
        Iterator iterator = result.iterator();
        list = new ArrayList<>();
        int count = 0;
        for(Result r : result) {
            count++;
            if(count == pageSize + 1) {
                startRow = new String(r.getRow());
                scan.setStartRow(startRow.getBytes());
                scan.setStopRow("006".getBytes());
                System.out.println("startRow" + startRow);
                break;
            } else {
                list.add(r);
            }
            startRow = new String(r.getRow());
            System.out.println(startRow);
            System.out.println(count);
        }
        return list;
    }

    public static void getPage(int pageNum, int pageSize) {
        System.out.println("hahha");
        // int pageNow = 0;
        Filter page = new PageFilter(pageSize + 1);
        int totalSize = pageNum * pageSize;
        Scan scan = new Scan();
        scan.setFilter(page);
        //pageNum = 3   需要扫描3页
        for (int i = 0; i < pageNum; i++) {

            try {
                ResultScanner rs = hTable.getScanner(scan);
                int count = 0;
                for (Result r : rs) {
                    count++;
                    if (count==pageSize + 1) {
                        startRow = new String(r.getRow());

                        scan.setStartRow(startRow.getBytes());
                        System.out.println("startRow" + startRow);
                        break;
                    }
                    startRow = new String(r.getRow());
                    System.out.println(startRow);
                    //把 r的所有的列都取出来     key-value age-20
                    for (KeyValue keyValue : r.list()) {
                        System.out.println("列："
                                + new String(keyValue.getQualifier()) + "====值:"
                                + new String(keyValue.getValue()));
                    }
                    System.out.println(count);


                }
                if (count < pageSize) {
                    break;
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }


    public static void getPageTwo(int pageNum, int pageSize) throws Exception {
        System.out.println("hahha");
        Filter page = new PageFilter(pageSize + 1);
        Scan scan = new Scan();
        scan.setFilter(page);
        ResultScanner rs = hTable.getScanner(scan);
        Result r = null;
        JSONObject json = new JSONObject();
        JSONArray array = new JSONArray();
        byte[] lastRow = null;
        int count = 0;
        while((r = rs.next()) != null) {
            lastRow = r.getRow();
            count++;
            if(count == pageSize + 1) {
                scan.setStartRow(lastRow);
                rs = hTable.getScanner(scan);
                count = 0;

                System.out.println("split page---------------");
            }
            System.out.println(Bytes.toString(lastRow));
            List<Cell> cells = r.listCells();
            JSONObject record = new JSONObject();
            for(int i=0;i<cells.size();i++){
                String key = Bytes.toString(CellUtil.cloneQualifier(cells.get(i)));
                String value = Bytes.toString(CellUtil.cloneValue(cells.get(i)));
                record.put(key, value);
                System.out.println(key);
                System.out.println(value);
                System.out.println("-------------------------");
            }
            array.put(record);
        }
        System.out.println(array.length());

    }

    public static void main(String args[]) {
        Paper paper = new Paper("hbase_1102");
        try {
            Paper.getPageTwo(3, 3);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

}
