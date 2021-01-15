package org.apache.phoenix.util;

import org.apache.phoenix.config.PhoenixClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author colar
 * @date 2021-01-15 下午5:01
 */
public class JdbcUtil {
    private static final Logger logger = LoggerFactory.getLogger(JdbcUtil.class);

    private static final String url = String.format("jdbc:phoenix:%s:%s",
            PhoenixClientConfig.HBASE_ZOOKEEPER_QUORUM,
            PhoenixClientConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT);
    private static final Properties prop = new Properties();

    /**
     * 执行非查询操作
     *
     * @param sql
     * @throws SQLException
     */
    public static void execute(Sql sql) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = createConnection(true);
            stmt = preparedStatement(conn, sql);
            stmt.execute();
        } finally {
            close(stmt);
            close(conn);
        }
    }

    /**
     * 批量执行非查询操作
     *
     * @param sqls
     * @throws SQLException
     */
    public static void execute(List<Sql> sqls) throws SQLException {
        Connection conn = null;
        List<PreparedStatement> stmts = new LinkedList<>();
        try {
            conn = createConnection(false);
            for (Sql sql : sqls) {
                PreparedStatement stmt = preparedStatement(conn, sql);
                stmt.execute();
                stmts.add(stmt);
            }
            conn.commit();
        } catch (SQLException throwables) {
            rollback(conn);
            throw throwables;
        } finally {
            for (PreparedStatement stmt : stmts) {
                close(stmt);
            }
            close(conn);
        }
    }

    /**
     * 执行查询操作
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public static List<Map<String, Object>> executeQuery(Sql sql) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = createConnection(true);
            stmt = preparedStatement(conn, sql);
            rs = stmt.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            List<Map<String, Object>> maps = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    map.put(meta.getColumnName(i), rs.getObject(i));
                }
                maps.add(map);
            }
            return maps;
        } finally {
            close(rs);
            close(stmt);
            close(conn);
        }
    }

    /**
     * 执行查询操作
     *
     * @param sql
     * @param clazz
     * @param <T>
     * @return
     * @throws SQLException
     */
    public static <T> List<T> executeQuery(Sql sql, Class<T> clazz) throws SQLException, IllegalAccessException, InstantiationException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = createConnection(true);
            stmt = preparedStatement(conn, sql);
            rs = stmt.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            List<T> list = new ArrayList<>();

            while (rs.next()) {
                T t = clazz.newInstance();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = meta.getColumnName(i);
                    Object obj = rs.getObject(i);

                    if (obj == null) {
                        continue;
                    }

                    Field declaredField;
                    try {
                        String[] split = columnName.toLowerCase().split("_");
                        StringBuilder propName = new StringBuilder();
                        for (int j = 0; j < split.length; j++) {
                            if (j == 0) {
                                propName.append(split[j]);
                            } else {
                                propName.append(split[j].substring(0, 1).toUpperCase()).append(split[j].substring(1));
                            }
                        }
                        declaredField = clazz.getDeclaredField(propName.toString());
                    } catch (NoSuchFieldException e1) {
                        continue;
                    }

                    declaredField.setAccessible(true);
                    declaredField.set(t, obj);
                }
                list.add(t);
            }

            return list;
        } finally {
            close(rs);
            close(stmt);
            close(conn);
        }
    }

    private static Connection createConnection(boolean autoCommit) throws SQLException {
        Connection conn = DriverManager.getConnection(JdbcUtil.url, JdbcUtil.prop);
        conn.setAutoCommit(autoCommit);
        return conn;
    }

    private static PreparedStatement preparedStatement(Connection conn, Sql sql) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(sql.getSql());
        if (sql.getParams() != null) {
            for (int i = 0; i < sql.getParams().size(); i++) {
                stmt.setObject(i + 1, sql.getParams().get(i));
            }
        }
        return stmt;
    }

    private static void rollback(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException throwables) {
                logger.error("回滚失败！", throwables);
            }
        }
    }

    private static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException throwables) {
                logger.error("关闭Connection失败！", throwables);
            }
        }
    }

    private static void close(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException throwables) {
                logger.error("关闭Statement失败！", throwables);
            }
        }
    }

    private static void close(PreparedStatement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException throwables) {
                logger.error("关闭PreparedStatement失败！", throwables);
            }
        }
    }

    private static void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException throwables) {
                logger.error("关闭ResultSet失败！", throwables);
            }
        }
    }

    public static class Sql {
        private String sql;
        private List<Object> params = new ArrayList<>();

        public Sql(String sql) {
            this.sql = sql;
        }

        public Sql(String sql, List<Object> params) {
            this.sql = sql;
            this.params = params;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public List<Object> getParams() {
            return params;
        }

        public void setParams(List<Object> params) {
            this.params = params;
        }

        public Sql addParams(Object... objs) {
            params.addAll(Arrays.asList(objs));
            return this;
        }
    }
}
