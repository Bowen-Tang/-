import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;

public class StreamingResultSet16Queue {
    private static final int QUEUE_SIZE = 20000;
    private static final int NUM_THREADS = 16;

    public static void main(String[] args) {
        BlockingQueue<RecordData>[] queues = new BlockingQueue[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            queues[i] = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        CountDownLatch latch = new CountDownLatch(NUM_THREADS);

        // 创建用于查询的连接
        try (Connection queryConn = DriverManager.getConnection("jdbc:mysql://10.2.103.156:3390/tpch?rewriteBatchedStatements=true&useServerPrepStmts=true&prepStmtCacheSqlLimit=100000000&useConfigs=maxPerformance", "t1", "123456");
             Statement stmt = queryConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            stmt.setFetchSize(Integer.MIN_VALUE);

            String query = "select /*+  read_from_storage(tiflash[customer,orders,lineitem,nation]) */" +
" c_custkey,c_name,sum(l_extendedprice * (1 - l_discount)) as revenue,c_acctbal,n_name,c_address,c_phone,c_comment," +
" min(c_mktsegment) c_mktsegment,min(l_partkey) l_partkey, min(l_suppkey) l_suppkey,min(l_linenumber) l_linenumber, min(l_quantity) l_quantity," +
" max(l_tax) l_tax, max(l_linestatus) l_linestatus, min(l_shipdate) l_shipdate, min(l_commitdate) l_commitdate, min(l_receiptdate) l_receiptdate, " +
" min(l_shipinstruct) l_shipinstruct, max(l_shipmode) l_shipmode, max(o_orderstatus) o_orderstatus, min(o_totalprice) o_totalprice, " +
" min(o_orderdate) o_orderdate, max(o_orderpriority) o_orderpriority, min(o_clerk) o_clerk, max(o_shippriority) o_shippriority, " +
" @@hostname as etl_host,current_user() as etl_user,current_date() as etl_date" +
" from tpch.customer,tpch.orders,tpch.lineitem,tpch.nation where " +
" c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1993-10-01' and o_orderdate < date '1994-10-01' " +
" and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey,c_name,c_acctbal,c_phone,n_name,c_address,c_comment order by c_custkey";

            ResultSet rs = stmt.executeQuery(query);

            // 创建用于写入的连接池
            ConnectionPool connectionPool = new ConnectionPool("jdbc:mysql://10.2.103.156:3390/test?rewriteBatchedStatements=true&useServerPrepStmts=true&prepStmtCacheSqlLimit=100000000&useConfigs=maxPerformance", "t1", "123456", NUM_THREADS);

            long startTime = System.currentTimeMillis(); // 记录开始时间

            for (int i = 0; i < NUM_THREADS; i++) {
                executorService.submit(new WriterThread(queues[i], connectionPool.getConnection(), latch));
            }

            int count = 0;
            while (rs.next()) {
                RecordData record = new RecordData(rs);
                int index = count % NUM_THREADS;
                queues[index].put(record);
                count++;

                if (count % QUEUE_SIZE == 0) {
                    System.out.println(getTimestamp() + " - Queued " + count + " rows.");
                }
            }

            // 关闭线程池
            executorService.shutdown();
            latch.await(); // 等待所有线程完成

            long endTime = System.currentTimeMillis(); // 记录结束时间
            long duration = endTime - startTime; // 计算总共花费的时间

            System.out.println("Total rows inserted: " + count);
            System.out.println("Total time taken: " + duration + " milliseconds");

        } catch (SQLException | InterruptedException | ConnectionPoolException e) {
            e.printStackTrace();
        }
    }

    private static String getTimestamp() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(new Date());
    }
}

class WriterThread implements Runnable {
    private final BlockingQueue<RecordData> queue;
    private final Connection connection;
    private final CountDownLatch latch;

    public WriterThread(BlockingQueue<RecordData> queue, Connection connection, CountDownLatch latch) {
        this.queue = queue;
        this.connection = connection;
        this.latch = latch;
    }

    @Override
    public void run() {
        try (PreparedStatement pstmt = connection.prepareStatement("INSERT INTO test.tpch_q10 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            int count = 0;

            while (true) {
                RecordData record = queue.poll(1, TimeUnit.SECONDS); // 使用poll避免阻塞，等待1秒
                if (record == null) {
                    break;
                }

                pstmt.setObject(1, record.getCustKey());
                pstmt.setObject(2, record.getName());
                pstmt.setObject(3, record.getRevenue());
                pstmt.setObject(4, record.getAcctBal());
                pstmt.setObject(5, record.getNationName());
                pstmt.setObject(6, record.getAddress());
                pstmt.setObject(7, record.getPhone());
                pstmt.setObject(8, record.getComment());
                pstmt.setObject(9, record.getMktSegment());
                pstmt.setObject(10, record.getPartKey());
                pstmt.setObject(11, record.getSuppKey());
                pstmt.setObject(12, record.getLineNumber());
                pstmt.setObject(13, record.getQuantity());
                pstmt.setObject(14, record.getTax());
                pstmt.setObject(15, record.getLineStatus());
                pstmt.setObject(16, record.getShipDate());
                pstmt.setObject(17, record.getCommitDate());
                pstmt.setObject(18, record.getReceiptDate());
                pstmt.setObject(19, record.getShipInstruct());
                pstmt.setObject(20, record.getShipMode());
                pstmt.setObject(21, record.getOrderStatus());
                pstmt.setObject(22, record.getTotalPrice());
                pstmt.setObject(23, record.getOrderDate());
                pstmt.setObject(24, record.getOrderPriority());
                pstmt.setObject(25, record.getClerk());
                pstmt.setObject(26, record.getShipPriority());
                pstmt.setObject(27, record.getEtlHost());
                pstmt.setObject(28, record.getEtlUser());
                pstmt.setObject(29, record.getEtlDate());

                pstmt.addBatch();
                count++;

                if (count % 4000 == 0) {
                    int[] result = pstmt.executeBatch();
                    System.out.println(getTimestamp() + " - Thread " + Thread.currentThread().getId() +
                            ": Inserted " + count + " rows.");
                }
            }

            int[] result = pstmt.executeBatch();
            System.out.println(getTimestamp() + " - Thread " + Thread.currentThread().getId() +
                    ": Inserted " + count + " rows.");

        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            latch.countDown(); // 减少计数
        }
    }

    private String getTimestamp() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(new Date());
    }
}

class RecordData {
    private final Object custKey;
    private final Object name;
    private final Object revenue;
    private final Object acctBal;
    private final Object nationName;
    private final Object address;
    private final Object phone;
    private final Object comment;
    private final Object mktSegment;
    private final Object partKey;
    private final Object suppKey;
    private final Object lineNumber;
    private final Object quantity;
    private final Object tax;
    private final Object lineStatus;
    private final Object shipDate;
    private final Object commitDate;
    private final Object receiptDate;
    private final Object shipInstruct;
    private final Object shipMode;
    private final Object orderStatus;
    private final Object totalPrice;
    private final Object orderDate;
    private final Object orderPriority;
    private final Object clerk;
    private final Object shipPriority;
    private final Object etlHost;
    private final Object etlUser;
    private final Object etlDate;

    public RecordData(ResultSet rs) throws SQLException {
        this.custKey = rs.getObject("c_custkey");
        this.name = rs.getObject("c_name");
        this.revenue = rs.getObject("revenue");
        this.acctBal = rs.getObject("c_acctbal");
        this.nationName = rs.getObject("n_name");
        this.address = rs.getObject("c_address");
        this.phone = rs.getObject("c_phone");
        this.comment = rs.getObject("c_comment");
        this.mktSegment = rs.getObject("c_mktsegment");
        this.partKey = rs.getObject("l_partkey");
        this.suppKey = rs.getObject("l_suppkey");
        this.lineNumber = rs.getObject("l_linenumber");
        this.quantity = rs.getObject("l_quantity");
        this.tax = rs.getObject("l_tax");
        this.lineStatus = rs.getObject("l_linestatus");
        this.shipDate = rs.getObject("l_shipdate");
        this.commitDate = rs.getObject("l_commitdate");
        this.receiptDate = rs.getObject("l_receiptdate");
        this.shipInstruct = rs.getObject("l_shipinstruct");
        this.shipMode = rs.getObject("l_shipmode");
        this.orderStatus = rs.getObject("o_orderstatus");
        this.totalPrice = rs.getObject("o_totalprice");
        this.orderDate = rs.getObject("o_orderdate");
        this.orderPriority = rs.getObject("o_orderpriority");
        this.clerk = rs.getObject("o_clerk");
        this.shipPriority = rs.getObject("o_shippriority");
        this.etlHost = rs.getObject("etl_host");
        this.etlUser = rs.getObject("etl_user");
        this.etlDate = rs.getObject("etl_date");
    }

    public Object getCustKey() {
        return custKey;
    }

    public Object getName() {
        return name;
    }

    public Object getRevenue() {
        return revenue;
    }

    public Object getAcctBal() {
        return acctBal;
    }

    public Object getNationName() {
        return nationName;
    }

    public Object getAddress() {
        return address;
    }

    public Object getPhone() {
        return phone;
    }

    public Object getComment() {
        return comment;
    }

    public Object getMktSegment() {
        return mktSegment;
    }

    public Object getPartKey() {
        return partKey;
    }

    public Object getSuppKey() {
        return suppKey;
    }

    public Object getLineNumber() {
        return lineNumber;
    }

    public Object getQuantity() {
        return quantity;
    }

    public Object getTax() {
        return tax;
    }

    public Object getLineStatus() {
        return lineStatus;
    }

    public Object getShipDate() {
        return shipDate;
    }

    public Object getCommitDate() {
        return commitDate;
    }

    public Object getReceiptDate() {
        return receiptDate;
    }

    public Object getShipInstruct() {
        return shipInstruct;
    }

    public Object getShipMode() {
        return shipMode;
    }

    public Object getOrderStatus() {
        return orderStatus;
    }

    public Object getTotalPrice() {
        return totalPrice;
    }

    public Object getOrderDate() {
        return orderDate;
    }

    public Object getOrderPriority() {
        return orderPriority;
    }

    public Object getClerk() {
        return clerk;
    }

    public Object getShipPriority() {
        return shipPriority;
    }

    public Object getEtlHost() {
        return etlHost;
    }

    public Object getEtlUser() {
        return etlUser;
    }

    public Object getEtlDate() {
        return etlDate;
    }
}

class ConnectionPool implements AutoCloseable {
    private final BlockingQueue<Connection> connections;

    public ConnectionPool(String url, String user, String password, int poolSize) throws SQLException {
        this.connections = new LinkedBlockingQueue<>(poolSize);
        initializeConnections(url, user, password, poolSize);
    }

    private void initializeConnections(String url, String user, String password, int poolSize) throws SQLException {
        try {
            for (int i = 0; i < poolSize; i++) {
                Connection connection = DriverManager.getConnection(url, user, password);
                connections.put(connection);
            }
        } catch (InterruptedException e) {
            throw new SQLException("Error initializing connections", e);
        }
    }

    public Connection getConnection() throws ConnectionPoolException {
        try {
            return connections.take();
        } catch (InterruptedException e) {
            throw new ConnectionPoolException("Error getting connection from the pool", e);
        }
    }

    @Override
    public void close() {
        for (Connection connection : connections) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

class ConnectionPoolException extends Exception {
    public ConnectionPoolException(String message, Throwable cause) {
        super(message, cause);
    }
}
