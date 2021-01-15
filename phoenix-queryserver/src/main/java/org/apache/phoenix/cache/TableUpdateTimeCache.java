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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 表更新时间缓存
 *
 * @author colar
 * @date 2021-01-18 下午2:15
 */
public class TableUpdateTimeCache extends PhoenixCache<String, Long> {
    private static final Logger logger = LoggerFactory.getLogger(TableUpdateTimeCache.class);
    private static final TableUpdateTimeCache instance = new TableUpdateTimeCache();

    private final Long interval = PhoenixClientConfig.PHOENIX_CLIENT_TABLE_UPDATE_TIME_INTERVAL;

    private final ThreadPoolExecutor pool;

    private TableUpdateTimeCache() {
        super();

        // 创建表更新时间表
        try {
            createTableUpdateTimeTable();
        } catch (SQLException throwables) {
            throw new RuntimeException("检测并创建表更新时间表失败！", throwables);
        }

        // 更新表更新时间缓存
        try {
            updateTableUpdateTimeCache();
        } catch (SQLException throwables) {
            throw new RuntimeException("更新表更新时间缓存失败！", throwables);
        }

        int corePoolSize = PhoenixClientConfig.PHOENIX_CLIENT_TABLE_UPDATE_TIME_CORE_POOL_SIZE;
        int maximumPoolSize = PhoenixClientConfig.PHOENIX_CLIENT_TABLE_UPDATE_TIME_MAXIMUM_POOL_SIZE;
        long keepAliveTime = PhoenixClientConfig.PHOENIX_CLIENT_TABLE_UPDATE_TIME_KEEP_ALIVE_TIME;
        int workQueue = PhoenixClientConfig.PHOENIX_CLIENT_TABLE_UPDATE_TIME_WORK_QUEUE;

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
                    Iterator<Cache.Entry<String, Long>> iterator = super.iterator();
                    while (iterator.hasNext()) {
                        Cache.Entry<String, Long> next = iterator.next();
                        sqls.add(getSql(next.getKey(), next.getValue()));
                    }
                    try {
                        JdbcUtil.execute(sqls);
                    } catch (SQLException throwables) {
                        logger.error("表更新时间缓存写入SYSTEM.TABLE_UPDATE_TIME表失败！", throwables);
                    }
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

    public static TableUpdateTimeCache getInstance() {
        return instance;
    }

    @Override
    protected Cache<String, Long> createCache() {
        Long heapSize = PhoenixClientConfig.PHOENIX_CLIENT_TABLE_UPDATE_HEAP_SIZE;
        ResourcePools pools = ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(heapSize, MemoryUnit.B).build();

        CacheConfiguration<String, Long> conf = CacheConfigurationBuilder
                .newCacheConfigurationBuilder(String.class, Long.class, pools)
                .build();

        return manager.createCache("tableUpdateTime", conf);
    }

    @Override
    public void put(String table, Long time) {
        if (interval == 0) {
            pool.execute(() -> {
                try {
                    JdbcUtil.execute(getSql(table, time));
                } catch (SQLException throwables) {
                    logger.error("表更新时间缓存写入SYSTEM.TABLE_UPDATE_TIME表失败！", throwables);
                }
            });
        }
        super.put(table, time);
    }

    /**
     * 检测并创建表更新时间表
     *
     * @throws SQLException
     */
    private void createTableUpdateTimeTable() throws SQLException {
        List<Map<String, Object>> catalogs = JdbcUtil.executeQuery(
                new JdbcUtil.Sql("SELECT * FROM SYSTEM.CATALOG WHERE TABLE_SCHEM = ? AND TABLE_NAME = ?")
                        .addParams("SYSTEM", "TABLE_UPDATE_TIME"));
        if (catalogs.isEmpty()) {
            logger.info("正在创建表更新时间表：SYSTEM.TABLE_UPDATE_TIME");
            JdbcUtil.execute(new JdbcUtil.Sql("CREATE TABLE SYSTEM.TABLE_UPDATE_TIME(TABLE_NAME VARCHAR NOT NULL PRIMARY KEY, UPDATE_TIME TIMESTAMP)"));
        }
    }

    /**
     * 根据表名和更新时间获取Sql
     *
     * @param table
     * @param time
     * @return
     */
    private JdbcUtil.Sql getSql(String table, long time) {
        return new JdbcUtil.Sql("UPSERT INTO SYSTEM.TABLE_UPDATE_TIME (TABLE_NAME, UPDATE_TIME) VALUES (?, ?)")
                .addParams(table, new Timestamp(time));
    }

    /**
     * 更新表更新时间缓存
     */
    private void updateTableUpdateTimeCache() throws SQLException {
        List<Map<String, Object>> maps = JdbcUtil.executeQuery(new JdbcUtil.Sql("SELECT * FROM SYSTEM.TABLE_UPDATE_TIME"));
        HashMap<String, Long> cacheMap = new HashMap<>(maps.size());
        for (Map<String, Object> map : maps) {
            String table = (String) map.get("TABLE_NAME");
            Timestamp timestamp = (Timestamp) map.get("UPDATE_TIME");
            cacheMap.put(table, timestamp.getTime());
        }
        super.putAll(cacheMap);
    }
}
