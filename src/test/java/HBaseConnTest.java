import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;
import site.it4u.bigdata.hbase.api.HBaseConn;

public class HBaseConnTest {

    @Test
    public void getConnTest() {
        Connection conn = HBaseConn.getHBaseConn();
        System.out.println(conn.isClosed());
        HBaseConn.closeConn();
        System.out.println(conn.isClosed());
    }

    @Test
    public void getTableTest() {
        try {
            Table table = HBaseConn.getTable("US_POPULATION");
            System.out.println(table.getName().getNameAsString());
            table.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
