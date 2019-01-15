package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class HbaseRlData {

    public static List<Map<String,String>> getRlData() {
        List list = new ArrayList();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String today = sdf.format(new Date());
        try {
            //创建hbase表名对象
            TableName tableName = TableName.valueOf("test");
            //创建连接的配
            Configuration conf = HBaseConfiguration.create();
            conf.addResource("hdfs://hadoop01/hbase");
            Connection connection=ConnectionFactory.createConnection();
            //获取表对象
            Table table=connection.getTable(tableName);
            Scan scan=new Scan();
          Filter filter=  new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(today+".*_"));
          scan.setFilter(filter);
          ResultScanner resultScanner=table.getScanner(scan);
          //  ResultScanner extends Closeable, Iterable<Result> { 是一种迭代器<Result>
          for (Result result:resultScanner){
              Map<String, String> map = new HashMap<>();
              //获取rowkey
              byte[] bytes= result.getRow();
              map.put("rowkey", Bytes.toString(bytes));
              Cell[] cells=result.rawCells();
              for (Cell cell:cells){
                 map.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell))) ;
              }
              list.add(map);

              }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;

    }

    public static void main(String[] args) {
      List<Map<String,String>> list=  getRlData();
        for (Map list1:list){
            System.out.println(list1.toString());

        }
    }
//    public List<Map<String, String>> auditorRt() {
//        String today = sdf.format(new Date());
//        List<Map<String, String>> list = new ArrayList<>();
//        try {
//            TableName tableName = TableName.valueOf("video_bc_real_amt_day");
//            Table table = HBaseUtil.getConnection().getTable(tableName);
//            Scan scan = new Scan();
//            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*_" + today + "_review$"));
//            scan.setFilter(filter);
//            ResultScanner scanner = table.getScanner(scan);
//            for (Result r : scanner) {
//                Map<String, String> map = new HashMap<>();
//                String rowKey = Bytes.toString(r.getRow());
//                String[] keys = rowKey.split(GlobalConstants.UNDERLINE);
//                if (keys.length != 3) {
//                    continue;
//                }
//                map.put("auditor", keys[0]);
//                for (Cell cell : r.rawCells()) {
//                    map.put(Bytes.toString(CellUtil.cloneQualifier(cell)), String.valueOf(Double.valueOf(Bytes.toString(CellUtil.cloneValue(cell))).intValue()));
//                }
//                list.add(map);
//            }
//            //计算all
//            Long allBcAmt = 0L;
//            for (Map<String, String> map : list) {
//                allBcAmt += Long.valueOf(map.get("person_bc_amt"));
//            }
//            Map<String, String> all = new HashMap<>();
//            all.put("auditor", "all");
//            all.put("person_bc_amt", allBcAmt.toString());
//            list.add(all);
//            //排序输出
//            Collections.sort(list, new Comparator<Map<String, String>>() {
//                @Override
//                public int compare(Map<String, String> o1, Map<String, String> o2) {
//                    return Integer.valueOf(o2.get("person_bc_amt")) - Integer.valueOf(o1.get("person_bc_amt"));
//                }
//            });
//            table.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return list;
//    }


}
