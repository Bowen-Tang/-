import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BatchLimit {
    private static String getTimestamp() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(new Date());
    }
    private static final int BATCH_SIZE = 10000;

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis(); // 记录开始时间

        // MySQL 连接配置
        String url = "jdbc:mysql://10.2.103.227:14000/tpch?rewriteBatchedStatements=true&useConfigs=maxPerformance";
        String user = "t1";
        String password = "123456";
        Connection connection = null;
        Connection writeConnection = null;

        try {
            connection = DriverManager.getConnection(url, user, password);
            writeConnection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(true);
            writeConnection.setAutoCommit(true);

            String query0 = "SELECT " +
                    "c_custkey, " +
                    "c_name, " +
                    "SUM(l_extendedprice * (1 - l_discount)) AS revenue, " +
                    "c_acctbal, " +
                    "n_name, " +
                    "c_address, " +
                    "c_phone, " +
                    "c_comment, " +
                    "MIN(C_MKTSEGMENT) AS c_mktsegment, " +
                    "MIN(L_PARTKEY) AS l_partkey, " +
                    "MIN(L_SUPPKEY) AS l_suppkey, " +
                    "MIN(L_LINENUMBER) AS l_linenumber, " +
                    "MIN(L_QUANTITY) AS l_quantity, " +
                    "MAX(L_TAX) AS l_tax, " +
                    "MAX(L_LINESTATUS) AS l_linestatus, " +
                    "MIN(L_SHIPDATE) AS l_shipdate, " +
                    "MIN(L_COMMITDATE) AS l_commitdate, " +
                    "MIN(L_RECEIPTDATE) AS l_receiptdate, " +
                    "MIN(L_SHIPINSTRUCT) AS l_shipinstruct, " +
                    "MAX(L_SHIPMODE) AS l_shipmode, " +
                    "MAX(O_ORDERSTATUS) AS o_orderstatus, " +
                    "MIN(O_TOTALPRICE) AS o_totalprice, " +
                    "MIN(O_ORDERDATE) AS o_orderdate, " +
                    "MAX(O_ORDERPRIORITY) AS o_orderpriority, " +
                    "MIN(O_CLERK) AS o_clerk, " +
                    "MAX(O_SHIPPRIORITY) AS o_shippriority, " +
                    "@@hostname AS etl_host, " +
                    "current_user() AS etl_user, " +
                    "current_date() AS etl_date " +
                    "FROM customer, orders, lineitem, nation " +
                    "WHERE c_custkey = o_custkey " +
                    "AND l_orderkey = o_orderkey " +
                    "AND o_orderdate >= date '1993-10-01' " +
                    "AND o_orderdate < date '1994-10-01' " +
                    "AND l_returnflag = 'R' " +
                    "AND c_nationkey = n_nationkey " +
                    "GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment " +
                    "ORDER BY c_custkey";
            String query = query0 + " LIMIT ?, ?";
            String countQuery = "SELECT COUNT(*) AS cnt FROM (" + query0 + ") t";

            String insertQuery = "INSERT INTO test.tpch_q10 " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            System.out.println(getTimestamp() + ": Begin get total rows");
            PreparedStatement countStmt = connection.prepareStatement(countQuery);
            ResultSet countRs = countStmt.executeQuery();
            countRs.next();
            int totalCount = countRs.getInt("cnt");
            int totalPages = (int) Math.ceil((double) totalCount / BATCH_SIZE);
                        System.out.println(getTimestamp() + ": total " + totalCount + " rows, " + totalPages +" pages");

            PreparedStatement stmt = connection.prepareStatement(query);
            PreparedStatement insertStmt = writeConnection.prepareStatement(insertQuery);

            for (int page = 0; page < totalPages; page++) {
                int offset = page * BATCH_SIZE;
                int limit = BATCH_SIZE;

                stmt.setInt(1, offset);
                stmt.setInt(2, limit);

                ResultSet rs = stmt.executeQuery();

                while (rs.next()) {
                    insertStmt.setObject(1, rs.getObject("c_custkey"));
                    insertStmt.setObject(2, rs.getObject("c_name"));
                    insertStmt.setObject(3, rs.getObject("revenue"));
                    insertStmt.setObject(4, rs.getObject("c_acctbal"));
                    insertStmt.setObject(5, rs.getObject("n_name"));
                    insertStmt.setObject(6, rs.getObject("c_address"));
                    insertStmt.setObject(7, rs.getObject("c_phone"));
                    insertStmt.setObject(8, rs.getObject("c_comment"));
                    insertStmt.setObject(9, rs.getObject("c_mktsegment"));
                    insertStmt.setObject(10, rs.getObject("l_partkey"));
                    insertStmt.setObject(11, rs.getObject("l_suppkey"));
                    insertStmt.setObject(12, rs.getObject("l_linenumber"));
                    insertStmt.setObject(13, rs.getObject("l_quantity"));
                    insertStmt.setObject(14, rs.getObject("l_tax"));
                    insertStmt.setObject(15, rs.getObject("l_linestatus"));
                    insertStmt.setObject(16, rs.getObject("l_shipdate"));
                    insertStmt.setObject(17, rs.getObject("l_commitdate"));
                    insertStmt.setObject(18, rs.getObject("l_receiptdate"));
                    insertStmt.setObject(19, rs.getObject("l_shipinstruct"));
                    insertStmt.setObject(20, rs.getObject("l_shipmode"));
                    insertStmt.setObject(21, rs.getObject("o_orderstatus"));
                    insertStmt.setObject(22, rs.getObject("o_totalprice"));
                    insertStmt.setObject(23, rs.getObject("o_orderdate"));
                    insertStmt.setObject(24, rs.getObject("o_orderpriority"));
                    insertStmt.setObject(25, rs.getObject("o_clerk"));
                    insertStmt.setObject(26, rs.getObject("o_shippriority"));
                    insertStmt.setObject(27, rs.getObject("etl_host"));
                    insertStmt.setObject(28, rs.getObject("etl_user"));
                    insertStmt.setObject(29, rs.getObject("etl_date"));

                    insertStmt.addBatch();
                }

                insertStmt.executeBatch();
                                System.out.println(getTimestamp() + ": Page " + (page + 1) + " of " + totalPages + " inserted");
                insertStmt.clearBatch();
            }

            long endTime = System.currentTimeMillis(); // 记录结束时间
            long duration = endTime - startTime; // 计算总共花费的时间

            System.out.println("Total rows inserted: " + totalCount);
            System.out.println("Total time taken: " + duration + " milliseconds");

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
                if (writeConnection != null) {
                    writeConnection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
