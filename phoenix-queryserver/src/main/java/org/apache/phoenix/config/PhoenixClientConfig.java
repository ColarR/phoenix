package org.apache.phoenix.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.util.JdbcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.net.URL;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author colar
 * @date 2021-01-14 下午6:17
 */
public class PhoenixClientConfig {
    private static Logger logger = LoggerFactory.getLogger(PhoenixClientConfig.class);

    // HBase相关配置
    /**
     * ZooKeeper IP地址
     */
    public static String HBASE_ZOOKEEPER_QUORUM = null;
    /**
     * ZooKeeper 端口地址
     */
    public static Integer HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = 2181;
    /**
     * HBase 超级用户名
     */
    public static String HBASE_SUPERUSER = null;

    // Phoenix Client相关配置（新增配置项）
    // HBase节点SSH配置
    /**
     * HBase 命令可执行节点IP地址
     */
    public static String PHOENIX_CLIENT_SSH_HOST = null;
    /**
     * HBase 命令可执行节点SSH端口
     */
    public static Integer PHOENIX_CLIENT_SSH_PORT = 22;
    /**
     * HBase 命令可执行节点用户名
     */
    public static String PHOENIX_CLIENT_SSH_USER = null;
    /**
     * HBase 命令可执行节点密码
     */
    public static String PHOENIX_CLIENT_SSH_PASSWORD = null;

    // Phoenix基于HBase ACL权限相关配置
    /**
     * Phoenix 超级用户的密码，用户名对应HBASE_SUPERUSER
     */
    public static String PHOENIX_CLIENT_HBASE_SUPERUSER_PASSWORD = null;

    // 异步索引自动构建相关配置
    /**
     * Yarn集群Restful节点地址
     */
    public static String PHOENIX_CLIENT_YARN_REST_HOST = null;
    /**
     * Yarn集群Restful端口
     */
    public static Integer PHOENIX_CLIENT_YARN_REST_PORT = 8088;
    /**
     * 创建异步索引线程池核心线程数量
     */
    public static Integer PHOENIX_CLIENT_ASYNC_INDEX_CORE_POOL_SIZE = 3;
    /**
     * 创建异步索引线程池最大线程数量
     */
    public static Integer PHOENIX_CLIENT_ASYNC_INDEX_MAXIMUM_POOL_SIZE = 10;
    /**
     * 创建异步索引线程池线程存活时间，单位毫秒
     */
    public static Long PHOENIX_CLIENT_ASYNC_INDEX_KEEP_ALIVE_TIME = 60000L;
    /**
     * 创建异步索引线程池线程队列数量
     */
    public static Integer PHOENIX_CLIENT_ASYNC_INDEX_WORK_QUEUE = 10;
    /**
     * 异步索引重试次数
     */
    public static Integer PHOENIX_CLIENT_ASYNC_INDEX_RETRY_NUM = 3;

    // 查询缓存相关配置
    /**
     * 查询缓存堆内存大小，单位Bytes
     */
    public static Long PHOENIX_CLIENT_QUERY_CACHE_HEAP_SIZE = 1073741824L;
    /**
     * 查询缓存失效时间（每次使用后时间刷新），单位毫秒
     */
    public static Long PHOENIX_CLIENT_QUERY_CACHE_IDLE_TIME = 604800000L;

    // 表更新时间缓存配置
    /**
     * 表更新时间堆内存大小，默认为100MB
     * 注意：表更新时间缓存存储表名和时间戳，如果表名占用100字节，则1MB可以存储10000个表，因此不需要修改该配置
     */
    public static Long PHOENIX_CLIENT_TABLE_UPDATE_HEAP_SIZE = 104857600L;
    /**
     * 表更新时间缓存写入SYSTEM.TABLE_UPDATE_TIME表的间隔，如果设置为0，则每次检测到表更新操作都会写入
     * 注意：该值越大，意味着故障时丢失的数据越多
     * 注意：该值越小，意味着越频繁地写入Phoenix表
     */
    public static Long PHOENIX_CLIENT_TABLE_UPDATE_TIME_INTERVAL = 5000L;
    /**
     * 表更新时间缓存写入SYSTEM.TABLE_UPDATE_TIME表的线程数量
     * 注意：只有当PHOENIX_CLIENT_TABLE_UPDATE_TIME_INTERVAL = 0时，该值才生效
     */
    public static Integer PHOENIX_CLIENT_TABLE_UPDATE_TIME_CORE_POOL_SIZE = 1;
    /**
     * 表更新时间缓存写入SYSTEM.TABLE_UPDATE_TIME表的最大线程数量
     */
    public static Integer PHOENIX_CLIENT_TABLE_UPDATE_TIME_MAXIMUM_POOL_SIZE = 10;
    /**
     * 表更新时间缓存写入SYSTEM.TABLE_UPDATE_TIME表的线程存活时间，单位毫秒
     */
    public static Long PHOENIX_CLIENT_TABLE_UPDATE_TIME_KEEP_ALIVE_TIME = 60000L;
    /**
     * 表更新时间缓存写入SYSTEM.TABLE_UPDATE_TIME表的线程队列大小
     */
    public static Integer PHOENIX_CLIENT_TABLE_UPDATE_TIME_WORK_QUEUE = 10;

    // 数据库审计日志缓存配置
    /**
     * 数据库审计日志开启
     * 注意：只有该值为true时，才会启用数据库审计日志
     */
    public static Boolean PHOENIX_CLIENT_AUDIT_ENABLE = false;
    /**
     * 数据库审计日志过滤前缀，设置为"*"代表所有操作都计入审计日志
     * 注意：只有符合设置前缀的SQL语句才会被记录
     */
    public static String PHOENIX_CLIENT_AUDIT_PRE = "*";
    /**
     * 数据库审计日志缓存对大小，默认为1GB
     * 注意：如果设置该值过小，会导致写入Phoenix表间隔期间可能会存在超出该设置的缓存，无法及时写入审计表中，导致审计日志丢失
     */
    public static Long PHOENIX_CLIENT_AUDIT_HEAP_SIZE = 1073741824L;
    /**
     * 数据库审计日志缓存写入SYSTEM.AUDIT表的间隔，如果设置为0，则每次检测到表更新操作都会写入
     * 注意：该值越大，意味着故障时丢失的数据越多
     * 注意：该值越小，意味着越频繁地写入Phoenix表
     * 注意：如果该值设置过大，会导致写入Phoenix表间隔期间可能会存在超出缓存堆大小的缓存，无法及时写入审计表中，导致审计日志丢失
     */
    public static Long PHOENIX_CLIENT_AUDIT_INTERVAL = 5000L;
    /**
     * 数据库审计日志缓存写入SYSTEM.AUDIT表的线程数量
     */
    public static Integer PHOENIX_CLIENT_AUDIT_CORE_POOL_SIZE = 1;
    /**
     * 数据库审计日志缓存写入SYSTEM.AUDIT表的最大线程数量
     */
    public static Integer PHOENIX_CLIENT_AUDIT_MAXIMUM_POOL_SIZE = 10;
    /**
     * 数据库审计日志缓存写入SYSTEM.AUDIT表的线程存活时间，单位毫秒
     */
    public static Long PHOENIX_CLIENT_AUDIT_KEEP_ALIVE_TIME = 60000L;
    /**
     * 数据库审计日志缓存写入SYSTEM.AUDIT表的线程队列大小
     */
    public static Integer PHOENIX_CLIENT_AUDIT_TIME_WORK_QUEUE = 10;

    /**
     * 查询时数据如果转换有异常，是否抛出
     * 注意：该配置主要是为了映射HBase表，HBase原始数据与Phoenix不匹配时，Phoenix查询会直接抛出异常
     * 注意：如果设置为false，则不会抛出异常，而是返回null
     */
    public static Boolean PHOENIX_CLIENT_ILLEGAL_THROW = true;

    static {
        // 初始化配置
        confInit();

        // 校验配置
        try {
            confCheck();
        } catch (IllegalAccessException e) {
            throw new RuntimeException("校验配置失败！", e);
        }

        // 创建用户表
        try {
            createUserTable();
        } catch (SQLException throwables) {
            throw new RuntimeException("检测并创建用户表失败！", throwables);
        }
    }

    /**
     * 检测并创建用户表
     *
     * @throws SQLException
     */
    private static void createUserTable() throws SQLException {
        List<Map<String, Object>> catalogs = JdbcUtil.executeQuery(
                new JdbcUtil.Sql("SELECT * FROM SYSTEM.CATALOG WHERE TABLE_SCHEM = ? AND TABLE_NAME = ?")
                        .addParams("SYSTEM", "USER"));
        if (catalogs.isEmpty()) {
            logger.info("正在创建用户表：SYSTEM.USER");
            JdbcUtil.execute(new JdbcUtil.Sql("CREATE TABLE SYSTEM.USER(USERNAME VARCHAR NOT NULL PRIMARY KEY, PASSWORD VARCHAR)"));
        }
    }

    /**
     * 校验配置并打印配置
     *
     * @throws IllegalAccessException
     */
    private static void confCheck() throws IllegalAccessException {
        logger.info("配置校验开始！");
        Class<PhoenixClientConfig> clazz = PhoenixClientConfig.class;
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            String fieldName = field.getName();
            Object o = field.get(null);
            if (o == null) {
                throw new RuntimeException("校验配置失败，未配置" + fieldName + "！");
            } else {
                logger.info(fieldName + " = " + o.toString());
            }
        }
        logger.info("配置校验结束！");
    }

    /**
     * 初始化配置，优先从环境变量中获取，其次从classpath中的hbase-site.xml中获取
     */
    private static void confInit() {
        Class<PhoenixClientConfig> clazz = PhoenixClientConfig.class;

        // 获取系统环境变量
        Map<String, String> env = System.getenv();

        // 获取$PHOENIX_HOME/bin/hbase-site.xml
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("hbase-site.xml");
        URL resource = conf.getResource("hbase-site.xml");
        logger.info("成功加载配置文件：" + resource.getPath());

        // 读取处于classpath中hbase-site.xml中的配置
        for (Map.Entry<String, String> c : conf) {
            try {
                setField(clazz, c.getKey().replaceAll("\\.", "_").toUpperCase(), c.getValue());
            } catch (NoSuchFieldException | IllegalAccessException ignored) {
            }
        }

        // 读取环境变量
        for (Map.Entry<String, String> e : env.entrySet()) {
            try {
                setField(clazz, e.getKey(), e.getValue());
            } catch (NoSuchFieldException | IllegalAccessException ignored) {
            }
        }
    }

    private static void setField(Class clazz, String fieldName, String value) throws NoSuchFieldException, IllegalAccessException {
        Field field = clazz.getDeclaredField(fieldName);

        field.setAccessible(true);
        String type = field.getType().getSimpleName();
        if ("Integer".equals(type)) {
            field.set(null, Integer.parseInt(value));
        } else if ("Boolean".equals(type)) {
            field.set(null, Boolean.parseBoolean(value.toUpperCase()));
        } else {
            field.set(null, value);
        }
    }
}
