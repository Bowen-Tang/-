import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StreamingResultSet {
    public static void main(String[] args) {
        String insertQuery = "INSERT INTO test.tpch_q10 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1); // If unable to load the driver, exit the program
        }

        System.out.println("MySQL JDBC Driver Registered!");
        String url = "jdbc:mysql://10.2.103.227:14000/tpch?rewriteBatchedStatements=true&useServerPrepStmts=true&prepStmtCacheSqlLimit=100000000&useConfigs=maxPerformance";
        String user = "t1";
        String password = "123456";

        int batchSize = 5000;
        int count = 0;
        long startTime = System.currentTimeMillis(); // Record start time

        try (
            // Create a connection for querying
            Connection queryConn = DriverManager.getConnection(url, user, password);
            Statement stmt = queryConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        ) {
            stmt.setFetchSize(Integer.MIN_VALUE); // Set to the minimum value for streaming

            String query = "SELECT /*+  read_from_storage(tiflash[customer,orders,lineitem,nation]) */" +
                    "c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue," +
                    "c_acctbal, n_name, c_address, c_phone, c_comment," +
                    "min(C_MKTSEGMENT), min(L_PARTKEY), min(L_SUPPKEY), min(L_LINENUMBER), min(L_QUANTITY), " +
                    "max(L_TAX), max(L_LINESTATUS), min(L_SHIPDATE), min(L_COMMITDATE), min(L_RECEIPTDATE), " +
                    "min(L_SHIPINSTRUCT), max(L_SHIPMODE), max(O_ORDERSTATUS), min(O_TOTALPRICE), " +
                    "min(O_ORDERDATE), max(O_ORDERPRIORITY), min(O_CLERK), max(O_SHIPPRIORITY), " +
                    "@@hostname, current_user(), current_date()" +
                    "FROM tpch.customer, tpch.orders, tpch.lineitem, tpch.nation " +
                    "WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey " +
                    "AND o_orderdate >= date '1993-10-01' AND o_orderdate < date '1994-10-01' " +
                    "AND l_returnflag = 'R' AND c_nationkey = n_nationkey " +
                    "GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment " +
                    "ORDER BY c_custkey";

            ResultSet rs = stmt.executeQuery(query);

            // Create a connection for insertion
            try (
                Connection insertConn = DriverManager.getConnection(url, user, password);
                PreparedStatement pstmt = insertConn.prepareStatement(insertQuery)
            ) {
                while (rs.next()) {
                    // Set data in PreparedStatement
                    pstmt.setObject(1, rs.getObject("c_custkey"));
                    pstmt.setObject(2, rs.getObject("c_name"));
                    pstmt.setObject(3, rs.getObject("revenue"));
                    pstmt.setObject(4, rs.getObject("c_acctbal"));
                    pstmt.setObject(5, rs.getObject("n_name"));
                    pstmt.setObject(6, rs.getObject("c_address"));
                    pstmt.setObject(7, rs.getObject("c_phone"));
                    pstmt.setObject(8, rs.getObject("c_comment"));
                    pstmt.setObject(9, rs.getObject("min(C_MKTSEGMENT)"));
                    pstmt.setObject(10, rs.getObject("min(L_PARTKEY)"));
                    pstmt.setObject(11, rs.getObject("min(L_SUPPKEY)"));
                    pstmt.setObject(12, rs.getObject("min(L_LINENUMBER)"));
                    pstmt.setObject(13, rs.getObject("min(L_QUANTITY)"));
                    pstmt.setObject(14, rs.getObject("max(L_TAX)"));
                    pstmt.setObject(15, rs.getObject("max(L_LINESTATUS)"));
                    pstmt.setObject(16, rs.getObject("min(L_SHIPDATE)"));
                    pstmt.setObject(17, rs.getObject("min(L_COMMITDATE)"));
                    pstmt.setObject(18, rs.getObject("min(L_RECEIPTDATE)"));
                    pstmt.setObject(19, rs.getObject("min(L_SHIPINSTRUCT)"));
                    pstmt.setObject(20, rs.getObject("max(L_SHIPMODE)"));
                    pstmt.setObject(21, rs.getObject("max(O_ORDERSTATUS)"));
                    pstmt.setObject(22, rs.getObject("min(O_TOTALPRICE)"));
                    pstmt.setObject(23, rs.getObject("min(O_ORDERDATE)"));
                    pstmt.setObject(24, rs.getObject("max(O_ORDERPRIORITY)"));
                    pstmt.setObject(25, rs.getObject("min(O_CLERK)"));
                    pstmt.setObject(26, rs.getObject("max(O_SHIPPRIORITY)"));
                    pstmt.setObject(27, rs.getObject("@@hostname"));
                    pstmt.setObject(28, rs.getObject("current_user()"));
                    pstmt.setObject(29, rs.getObject("current_date()"));

                    pstmt.addBatch();
                    count++;

                    if (count % batchSize == 0) {
                        // Execute batch insert operation
                        int[] result = pstmt.executeBatch();
                        System.out.println(getTimestamp() + " - Inserted " + count + " rows.");
                    }
                }

                // Execute the remaining insert operations
                int[] result = pstmt.executeBatch();
                System.out.println(getTimestamp() + " - Inserted " + count + " rows.");

            } catch (SQLException e) {
                e.printStackTrace();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis(); // Record end time
        long duration = endTime - startTime; // Calculate total time spent

        System.out.println("Total rows inserted: " + count);
        System.out.println("Total time taken: " + duration + " milliseconds");
    }

    private static String getTimestamp() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(new Date());
    }
}
}
