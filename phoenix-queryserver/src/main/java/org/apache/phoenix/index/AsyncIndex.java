package org.apache.phoenix.index;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jcraft.jsch.JSchException;
import org.apache.phoenix.config.PhoenixClientConfig;
import org.apache.phoenix.parse.BindableStatement;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.util.HttpUtil;
import org.apache.phoenix.util.JSchUtil;
import org.apache.phoenix.util.JdbcUtil;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author colar
 * @date 2021-01-06 下午1:59
 */
public class AsyncIndex {
    private final static Logger logger = LoggerFactory.getLogger(AsyncIndex.class);
    private final static AsyncIndex instance = new AsyncIndex();

    /**
     * 创建异步索引的Shell命令
     */
    private final String CMD = "source /etc/profile && hbase org.apache.phoenix.mapreduce.index.IndexTool %s %s %s --output-path . 2>&1";

    private final String HOST = PhoenixClientConfig.PHOENIX_CLIENT_SSH_HOST;
    private final Integer PORT = PhoenixClientConfig.PHOENIX_CLIENT_SSH_PORT;
    private final String USER = PhoenixClientConfig.PHOENIX_CLIENT_SSH_USER;
    private final String PASSWORD = PhoenixClientConfig.PHOENIX_CLIENT_SSH_PASSWORD;

    private final Integer CORE_POOL_SIZE = PhoenixClientConfig.PHOENIX_CLIENT_ASYNC_INDEX_CORE_POOL_SIZE;
    private final Integer MAXIMUM_POOL_SIZE = PhoenixClientConfig.PHOENIX_CLIENT_ASYNC_INDEX_MAXIMUM_POOL_SIZE;
    private final Long KEEP_ALIVE_TIME = PhoenixClientConfig.PHOENIX_CLIENT_ASYNC_INDEX_KEEP_ALIVE_TIME;
    private final Integer WORK_QUEUE = PhoenixClientConfig.PHOENIX_CLIENT_ASYNC_INDEX_WORK_QUEUE;

    private final Integer RETRY_NUM = PhoenixClientConfig.PHOENIX_CLIENT_ASYNC_INDEX_RETRY_NUM;

    private final String YARN_REST_ADDRESS = "http://" + PhoenixClientConfig.PHOENIX_CLIENT_YARN_REST_HOST + ":"
            + PhoenixClientConfig.PHOENIX_CLIENT_YARN_REST_PORT;

    private final ThreadPoolExecutor POOL = new ThreadPoolExecutor(CORE_POOL_SIZE,
            MAXIMUM_POOL_SIZE,
            KEEP_ALIVE_TIME,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(WORK_QUEUE),
            new ThreadFactoryBuilder().build());

    private AsyncIndex() {
        try {
            restore();
        } catch (IOException | SQLException e) {
            throw new RuntimeException("异步索引故障恢复失败！", e);
        } catch (JSONException e) {
            if ("JSONObject[\"apps\"] is not a JSONObject.".equals(e.getMessage())) {
                logger.warn("异步索引故障恢复失败，可能原因是Yarn中从来没有创建过任务导致Json解析异常！", e);
            } else {
                throw new RuntimeException("异步索引故障恢复失败！", e);
            }
        }
    }

    public static AsyncIndex getInstance() {
        return instance;
    }

    /**
     * 故障恢复，重启时候会检测未创建的索引进行构建
     *
     * @throws IOException
     * @throws JSONException
     * @throws SQLException
     */
    private void restore() throws IOException, JSONException, SQLException {
        // 获取当前正在运行的MapReduce任务
        ArrayList<String> running = new ArrayList<>();
        String body = HttpUtil.getStringBody(YARN_REST_ADDRESS + "/ws/v1/cluster/apps");
        JSONArray apps = new JSONObject(body).getJSONObject("apps").getJSONArray("app");
        for (int i = 0; i < apps.length(); i++) {
            JSONObject app = apps.getJSONObject(i);
            if ("RUNNING".equals(app.getString("state"))) {
                String name = app.getString("name");
                running.add(name);
            }
        }

        // 获取尚未构建完成的索引并开始创建
        List<Map<String, Object>> indexes = JdbcUtil.executeQuery(new JdbcUtil.Sql("SELECT * FROM SYSTEM.CATALOG WHERE TABLE_TYPE = 'i' AND INDEX_STATE = 'b'"));

        for (Map<String, Object> index : indexes) {
            String schemaName = (String) index.get("TABLE_SCHEM");
            String tableName = (String) index.get("DATA_TABLE_NAME");
            String indexName = (String) index.get("TABLE_NAME");
            String name = schemaName == null ?
                    "PHOENIX_" + "null" + "." + tableName + "_INDX_" + indexName :
                    "PHOENIX_" + schemaName + "." + tableName + "_INDX_" + indexName;
            if (!running.contains(name)) {
                run(schemaName, tableName, indexName);
            }
        }
    }

    /**
     * 创建异步索引
     * 如果索引创建失败，会根据配置进行重试
     * 如果重试后依然失败，则会更新索引状态为DISABLE
     *
     * @param schema
     * @param table
     * @param index
     */
    private void run(final String schema, final String table, final String index) {
        final String shell = schema != null ?
                String.format(CMD, "--schema " + schema, "--data-table " + table, "--index-table " + index) :
                String.format(CMD, "", "--data-table " + table, "--index-table " + index);
        try {
            POOL.execute(() -> {
                logger.info("开始创建异步索引，schema = {}, table = {}, index = {}", schema, table, index);
                for (int i = 0; i < RETRY_NUM + 1; i++) {
                    try {
                        // 执行MapReduce创建异步索引
                        String result = JSchUtil.exec(HOST, PORT, USER, PASSWORD, shell);
                        if (result.contains(
                                String.format("Updated the status of the index %s to ACTIVE", index))) {
                            logger.info("异步索引构建完成，schema = {}, table = {}, index = {}", schema, table, index);
                            return;
                        } else {
                            throw new SQLException("异步索引构建失败，shell命令输出：" + result);
                        }
                    } catch (JSchException | IOException | SQLException e) {
                        // 故障处理
                        logger.error("异步构建索引失败，schema = " + schema + ", table = " + table + ", index = " + index + "，5秒后进行第" + (i + 1) + "次重试！", e);
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException interruptedException) {
                            logger.error("Thread.sleep()失败！", interruptedException);
                        }

                        if (i == RETRY_NUM) {
                            logger.error("索引构建失败，开始修改索引状态为DISABLE！", e);
                            setDisableState(schema, index);
                        }
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            logger.error("索引构建失败，开始修改索引状态为DISABLE！", e);
            setDisableState(schema, index);
        }
    }

    /**
     * 修改索引状态为DISABLE
     *
     * @param schema
     * @param index
     */
    private void setDisableState(String schema, String index) {
        try {
            JdbcUtil.execute(new JdbcUtil.Sql("UPSERT INTO SYSTEM.CATALOG (TENANT_ID, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, COLUMN_FAMILY, INDEX_STATE) " +
                    "SELECT TENANT_ID, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, COLUMN_FAMILY, 'x' FROM SYSTEM.CATALOG " +
                    "WHERE TENANT_ID IS NULL AND TABLE_SCHEM " + (schema == null ? "IS NULL" : ("= '" + schema + "'")) + " AND TABLE_NAME = ? AND COLUMN_NAME IS NULL AND COLUMN_FAMILY IS NULL")
                    .addParams(index));
        } catch (SQLException throwables) {
            logger.error("修改索引状态为DISABLE失败，schema = {}，index = {}", schema, index);
        }
    }

    /**
     * 检测并创建异步索引
     *
     * @param stmt
     */
    public void create(BindableStatement stmt) {
        String schema = ((CreateIndexStatement) stmt).getTable().getName().getSchemaName();
        String table = ((CreateIndexStatement) stmt).getTable().getName().getTableName();
        String index = ((CreateIndexStatement) stmt).getIndexTableName().getTableName();

        logger.info("检测到异步索引构建，schema = {}, table = {}, index = {}！", schema, table, index);
        run(schema, table, index);
    }
}
