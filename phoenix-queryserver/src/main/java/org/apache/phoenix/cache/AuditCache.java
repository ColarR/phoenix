package org.apache.phoenix.cache;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.phoenix.config.PhoenixClientConfig;
import org.apache.phoenix.util.JdbcUtil;
import org.ehcache.Cache;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author colar
 * @date 2021-01-21 下午1:51
 */
public class AuditCache extends PhoenixCache<AuditCache.Key, Object> {
    private static final Logger logger = LoggerFactory.getLogger(AuditCache.class);
    private static final AuditCache instance = new AuditCache();

    private final boolean enable = PhoenixClientConfig.PHOENIX_CLIENT_AUDIT_ENABLE;
    private final List<String> pres;
    private final long interval = PhoenixClientConfig.PHOENIX_CLIENT_AUDIT_INTERVAL;

    private ThreadPoolExecutor pool;

    public static AuditCache getInstance() {
        return instance;
    }

    private AuditCache() {
        super();

        pres = new ArrayList<>();
        String[] split = PhoenixClientConfig.PHOENIX_CLIENT_AUDIT_PRE.split(",");
        for (String s : split) {
            pres.add(s.trim().toUpperCase());
        }

        int corePoolSize = PhoenixClientConfig.PHOENIX_CLIENT_AUDIT_CORE_POOL_SIZE;
        int maximumPoolSize = PhoenixClientConfig.PHOENIX_CLIENT_AUDIT_MAXIMUM_POOL_SIZE;
        long keepAliveTime = PhoenixClientConfig.PHOENIX_CLIENT_AUDIT_KEEP_ALIVE_TIME;
        int workQueue = PhoenixClientConfig.PHOENIX_CLIENT_AUDIT_TIME_WORK_QUEUE;

        if (enable) {
            // 创建表更新时间表
            try {
                createAuditTable();
            } catch (SQLException throwables) {
                throw new RuntimeException("检测并创建审计表失败！", throwables);
            }

            if (interval != 0) {
                // 启动更新表线程
                pool = new ThreadPoolExecutor(1,
                        maximumPoolSize,
                        keepAliveTime,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(workQueue),
                        new ThreadFactoryBuilder().build());

                pool.execute(() -> {
                    while (true) {
                        LinkedList<JdbcUtil.Sql> sqls = new LinkedList<>();
                        Iterator<Cache.Entry<Key, Object>> iterator = super.iterator();
                        LinkedHashSet<Key> keys = new LinkedHashSet<>();
                        while (iterator.hasNext()) {
                            Cache.Entry<Key, Object> next = iterator.next();
                            Key key = next.getKey();
                            sqls.add(getSql(key));
                            keys.add(key);
                        }

                        try {
                            JdbcUtil.execute(sqls);
                        } catch (SQLException throwables) {
                            logger.error("审计缓存缓存写入SYSTEM.AUDIT表失败！", throwables);
                        }

                        super.removeAll(keys);

                        try {
                            Thread.sleep(interval);
                        } catch (InterruptedException e) {
                            logger.error("Thread.sleep()执行失败！", e);
                        }
                    }
                });
            } else {
                pool = new ThreadPoolExecutor(corePoolSize,
                        maximumPoolSize,
                        keepAliveTime,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(workQueue),
                        new ThreadFactoryBuilder().build());
            }
        }
    }

    public boolean isEnable() {
        return enable;
    }

    public List<String> getPres() {
        return pres;
    }

    @Override
    protected Cache<Key, Object> createCache() {
        Long heapSize = PhoenixClientConfig.PHOENIX_CLIENT_AUDIT_HEAP_SIZE;
        ResourcePools pools = ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(heapSize, MemoryUnit.B).build();

        CacheConfiguration<Key, Object> conf = CacheConfigurationBuilder
                .newCacheConfigurationBuilder(Key.class, Object.class, pools)
                .build();

        return manager.createCache("audit", conf);
    }

    @Override
    public void put(Key key, Object o) {
        if (interval == 0) {
            pool.execute(() -> {
                try {
                    JdbcUtil.execute(getSql(key));
                } catch (SQLException throwables) {
                    logger.error("审计缓存缓存写入SYSTEM.AUDIT表失败！", throwables);
                }
            });
        } else {
            super.put(key, o);
        }
    }

    private void createAuditTable() throws SQLException {
        List<Map<String, Object>> catalogs = JdbcUtil.executeQuery(
                new JdbcUtil.Sql("SELECT * FROM SYSTEM.CATALOG WHERE TABLE_SCHEM = ? AND TABLE_NAME = ?")
                        .addParams("SYSTEM", "AUDIT"));
        if (catalogs.isEmpty()) {
            logger.info("正在创建审计表：SYSTEM.AUDIT");
            JdbcUtil.execute(new JdbcUtil.Sql("CREATE TABLE SYSTEM.AUDIT(" +
                    "TIME TIMESTAMP NOT NULL, " +
                    "USER VARCHAR, " +
                    "SQL VARCHAR, " +
                    "PARAMS VARCHAR, " +
                    "SUCCESS BOOLEAN, " +
                    "ERROR_MSG VARCHAR " +
                    "CONSTRAINT PK PRIMARY KEY(TIME, USER))"));
        }
    }

    /**
     * 根据Key获取Sql对象
     *
     * @param key
     * @return
     */
    private JdbcUtil.Sql getSql(Key key) {
        return new JdbcUtil.Sql("UPSERT INTO SYSTEM.AUDIT " +
                "(TIME, USER, SQL, PARAMS, SUCCESS, ERROR_MSG) " +
                "VALUES (?, ?, ?, ?, ?, ?)")
                .addParams(new Timestamp(key.getTime()),
                        key.getUser(),
                        key.getSql(),
                        key.getParams().toString(),
                        key.getSuccess(),
                        key.getErrorMsg());
    }

    public static class Key {
        private Long time;
        private String user;
        private String sql;
        private List<Object> params;
        private Boolean success;
        private String errorMsg;

        public Key(Long time, String user, String sql, List<Object> params, Boolean success, String errorMsg) {
            this.time = time;
            this.user = user;
            this.sql = sql;
            this.params = params;
            this.success = success;
            this.errorMsg = errorMsg;
        }

        public Long getTime() {
            return time;
        }

        public void setTime(Long time) {
            this.time = time;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
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

        public Boolean getSuccess() {
            return success;
        }

        public void setSuccess(Boolean success) {
            this.success = success;
        }

        public String getErrorMsg() {
            return errorMsg;
        }

        public void setErrorMsg(String errorMsg) {
            this.errorMsg = errorMsg;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return Objects.equals(time, key.time) &&
                    Objects.equals(user, key.user) &&
                    Objects.equals(sql, key.sql) &&
                    Objects.equals(params, key.params) &&
                    Objects.equals(success, key.success) &&
                    Objects.equals(errorMsg, key.errorMsg);
        }

        @Override
        public int hashCode() {
            return Objects.hash(time, user, sql, params, success, errorMsg);
        }
    }
}
