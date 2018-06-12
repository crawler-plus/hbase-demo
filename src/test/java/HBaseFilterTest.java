import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import site.it4u.bigdata.hbase.api.HBaseUtil;

import java.util.Arrays;

public class HBaseFilterTest {

    @Test
    public void rowFilterTest() {
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("rowkey2")));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                Arrays.asList(filter));
        ResultScanner scanner = HBaseUtil.getScanner("FileT",
                "rowkey2", "rowkey2", filterList);
        if(scanner != null) {
            scanner.forEach(result -> {
                System.out.println("rowkey=" + Bytes.toString(result.getRow()));
                System.out.println("fileName=" + Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"),
                        Bytes.toBytes("name"))));
            });
            scanner.close();
        }
    }
    
    @Test
    public void prefixFilterTest() {
        Filter filter = new PrefixFilter(Bytes.toBytes("rowkey2"));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                Arrays.asList(filter));
        ResultScanner scanner = HBaseUtil.getScanner("FileT",
                "rowkey2", "rowkey2", filterList);
        if(scanner != null) {
            scanner.forEach(result -> {
                System.out.println("rowkey=" + Bytes.toString(result.getRow()));
                System.out.println("fileName=" + Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"),
                        Bytes.toBytes("name"))));
            });
            scanner.close();
        }
    }

    @Test
    public void keyOnlyFilterTest() {
        Filter filter = new KeyOnlyFilter(true);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                Arrays.asList(filter));
        ResultScanner scanner = HBaseUtil.getScanner("FileT",
                "rowkey2", "rowkey2", filterList);
        if(scanner != null) {
            scanner.forEach(result -> {
                System.out.println("rowkey=" + Bytes.toString(result.getRow()));
                System.out.println("fileName=" + Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"),
                        Bytes.toBytes("name"))));
            });
            scanner.close();
        }
    }

    @Test
    public void columnPrefixFilterTest() {
        Filter filter = new ColumnPrefixFilter(Bytes.toBytes("nam"));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                Arrays.asList(filter));
        ResultScanner scanner = HBaseUtil.getScanner("FileT",
                "rowkey2", "rowkey2", filterList);
        if(scanner != null) {
            scanner.forEach(result -> {
                System.out.println("rowkey=" + Bytes.toString(result.getRow()));
                System.out.println("fileName=" + Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"),
                        Bytes.toBytes("name"))));
                System.out.println("fileType=" + Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"),
                        Bytes.toBytes("type"))));
            });
            scanner.close();
        }
    }
}
