/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.avatica.jdbc;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchConnectionException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.metrics.Gauge;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Requests;
import org.apache.calcite.avatica.remote.ProtobufMeta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.Unsafe;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.cache.AuditCache;
import org.apache.phoenix.cache.QueryCache;
import org.apache.phoenix.cache.TableUpdateTimeCache;
import org.apache.phoenix.config.PhoenixClientConfig;
import org.apache.phoenix.index.AsyncIndex;
import org.apache.phoenix.parse.BindableStatement;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.parse.DMLStatement;
import org.apache.phoenix.parse.DeleteStatement;
import org.apache.phoenix.parse.DropTableStatement;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.UpsertStatement;
import org.apache.phoenix.util.JdbcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.calcite.avatica.remote.MetricsHelper.concat;

/**
 * Implementation of {@link Meta} upon an existing JDBC data source.
 */
public class JdbcMeta implements ProtobufMeta {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcMeta.class);

    private static final String CONN_CACHE_KEY_BASE = "avatica.connectioncache";

    private static final String STMT_CACHE_KEY_BASE = "avatica.statementcache";

    /**
     * Special value for {@code Statement#getLargeMaxRows()} that means fetch
     * an unlimited number of rows in a single batch.
     *
     * <p>Any other negative value will return an unlimited number of rows but
     * will do it in the default batch size, namely 100.
     */
    public static final int UNLIMITED_COUNT = -2;

    // End of constants, start of member variables

    final Calendar calendar = Unsafe.localCalendar();

    /**
     * Generates ids for statements. The ids are unique across all connections
     * created by this JdbcMeta.
     */
    private final AtomicInteger statementIdGenerator = new AtomicInteger();

    private final String url;
    private final Properties info;
    private final Cache<String, Connection> connectionCache;
    private final Cache<Integer, StatementInfo> statementCache;
    private final Cache<String, String> userCache;
    private final MetricsSystem metrics;

    /**
     * 查询缓存实例
     */
    private final QueryCache queryCache = QueryCache.getInstance();
    /**
     * 表更新时间缓存实例
     */
    private final TableUpdateTimeCache tableUpdateTimeCache = TableUpdateTimeCache.getInstance();
    /**
     * 审计日志缓存实例
     */
    private final AuditCache auditCache = AuditCache.getInstance();
    /**
     * 异步索引实例
     */
    private final AsyncIndex asyncIndex = AsyncIndex.getInstance();

    /**
     * Creates a JdbcMeta.
     *
     * @param url a database url of the form
     *            <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>
     */
    public JdbcMeta(String url) throws SQLException {
        this(url, new Properties());
    }

    /**
     * Creates a JdbcMeta.
     *
     * @param url      a database url of the form
     *                 <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>
     * @param user     the database user on whose behalf the connection is being
     *                 made
     * @param password the user's password
     */
    public JdbcMeta(final String url, final String user, final String password)
            throws SQLException {
        this(url, new Properties() {
            {
                put("user", user);
                put("password", password);
            }
        });
    }

    public JdbcMeta(String url, Properties info) throws SQLException {
        this(url, info, NoopMetricsSystem.getInstance());
    }

    /**
     * Creates a JdbcMeta.
     *
     * @param url  a database url of the form
     *             <code> jdbc:<em>subprotocol</em>:<em>subname</em></code>
     * @param info a list of arbitrary string tag/value pairs as
     *             connection arguments; normally at least a "user" and
     *             "password" property should be included
     */
    public JdbcMeta(String url, Properties info, MetricsSystem metrics)
            throws SQLException {
        this.url = url;
        this.info = info;
        this.metrics = Objects.requireNonNull(metrics);

        int concurrencyLevel = Integer.parseInt(
                info.getProperty(ConnectionCacheSettings.CONCURRENCY_LEVEL.key(),
                        ConnectionCacheSettings.CONCURRENCY_LEVEL.defaultValue()));
        int initialCapacity = Integer.parseInt(
                info.getProperty(ConnectionCacheSettings.INITIAL_CAPACITY.key(),
                        ConnectionCacheSettings.INITIAL_CAPACITY.defaultValue()));
        long maxCapacity = Long.parseLong(
                info.getProperty(ConnectionCacheSettings.MAX_CAPACITY.key(),
                        ConnectionCacheSettings.MAX_CAPACITY.defaultValue()));
        long connectionExpiryDuration = Long.parseLong(
                info.getProperty(ConnectionCacheSettings.EXPIRY_DURATION.key(),
                        ConnectionCacheSettings.EXPIRY_DURATION.defaultValue()));
        TimeUnit connectionExpiryUnit = TimeUnit.valueOf(
                info.getProperty(ConnectionCacheSettings.EXPIRY_UNIT.key(),
                        ConnectionCacheSettings.EXPIRY_UNIT.defaultValue()));
        this.connectionCache = CacheBuilder.newBuilder()
                .concurrencyLevel(concurrencyLevel)
                .initialCapacity(initialCapacity)
                .maximumSize(maxCapacity)
                .expireAfterAccess(connectionExpiryDuration, connectionExpiryUnit)
                .removalListener(new ConnectionExpiryHandler())
                .build();
        LOG.debug("instantiated connection cache: {}", connectionCache.stats());

        concurrencyLevel = Integer.parseInt(
                info.getProperty(StatementCacheSettings.CONCURRENCY_LEVEL.key(),
                        StatementCacheSettings.CONCURRENCY_LEVEL.defaultValue()));
        initialCapacity = Integer.parseInt(
                info.getProperty(StatementCacheSettings.INITIAL_CAPACITY.key(),
                        StatementCacheSettings.INITIAL_CAPACITY.defaultValue()));
        maxCapacity = Long.parseLong(
                info.getProperty(StatementCacheSettings.MAX_CAPACITY.key(),
                        StatementCacheSettings.MAX_CAPACITY.defaultValue()));
        connectionExpiryDuration = Long.parseLong(
                info.getProperty(StatementCacheSettings.EXPIRY_DURATION.key(),
                        StatementCacheSettings.EXPIRY_DURATION.defaultValue()));
        connectionExpiryUnit = TimeUnit.valueOf(
                info.getProperty(StatementCacheSettings.EXPIRY_UNIT.key(),
                        StatementCacheSettings.EXPIRY_UNIT.defaultValue()));
        this.statementCache = CacheBuilder.newBuilder()
                .concurrencyLevel(concurrencyLevel)
                .initialCapacity(initialCapacity)
                .maximumSize(maxCapacity)
                .expireAfterAccess(connectionExpiryDuration, connectionExpiryUnit)
                .removalListener(new StatementExpiryHandler())
                .build();

        this.userCache = CacheBuilder.newBuilder()
                .concurrencyLevel(concurrencyLevel)
                .initialCapacity(initialCapacity)
                .maximumSize(maxCapacity)
                .expireAfterAccess(connectionExpiryDuration, connectionExpiryUnit)
                .removalListener(new UserExpiryHandler())
                .build();

        LOG.debug("instantiated statement cache: {}", statementCache.stats());

        // Register some metrics
        this.metrics.register(concat(JdbcMeta.class, "ConnectionCacheSize"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return connectionCache.size();
            }
        });

        this.metrics.register(concat(JdbcMeta.class, "StatementCacheSize"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return statementCache.size();
            }
        });
    }

    // For testing purposes
    protected AtomicInteger getStatementIdGenerator() {
        return statementIdGenerator;
    }

    // For testing purposes
    protected Cache<Integer, StatementInfo> getStatementCache() {
        return statementCache;
    }

    /**
     * Converts from JDBC metadata to Avatica columns.
     */
    protected static List<ColumnMetaData>
    columns(ResultSetMetaData metaData) throws SQLException {
        if (metaData == null) {
            return Collections.emptyList();
        }
        final List<ColumnMetaData> columns = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            final SqlType sqlType = SqlType.valueOf(metaData.getColumnType(i));
            final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(sqlType.internal);
            final ColumnMetaData.AvaticaType t;
            if (sqlType == SqlType.ARRAY || sqlType == SqlType.STRUCT || sqlType == SqlType.MULTISET) {
                ColumnMetaData.AvaticaType arrayValueType = ColumnMetaData.scalar(Types.JAVA_OBJECT,
                        metaData.getColumnTypeName(i), ColumnMetaData.Rep.OBJECT);
                t = ColumnMetaData.array(arrayValueType, metaData.getColumnTypeName(i), rep);
            } else {
                t = ColumnMetaData.scalar(metaData.getColumnType(i), metaData.getColumnTypeName(i), rep);
            }
            ColumnMetaData md =
                    new ColumnMetaData(i - 1, metaData.isAutoIncrement(i),
                            metaData.isCaseSensitive(i), metaData.isSearchable(i),
                            metaData.isCurrency(i), metaData.isNullable(i),
                            metaData.isSigned(i), metaData.getColumnDisplaySize(i),
                            metaData.getColumnLabel(i), metaData.getColumnName(i),
                            metaData.getSchemaName(i), metaData.getPrecision(i),
                            metaData.getScale(i), metaData.getTableName(i),
                            metaData.getCatalogName(i), t, metaData.isReadOnly(i),
                            metaData.isWritable(i), metaData.isDefinitelyWritable(i),
                            metaData.getColumnClassName(i));
            columns.add(md);
        }
        return columns;
    }

    /**
     * Converts from JDBC metadata to Avatica parameters
     */
    protected static List<AvaticaParameter> parameters(ParameterMetaData metaData)
            throws SQLException {
        if (metaData == null) {
            return Collections.emptyList();
        }
        final List<AvaticaParameter> params = new ArrayList<>();
        for (int i = 1; i <= metaData.getParameterCount(); i++) {
            params.add(
                    new AvaticaParameter(metaData.isSigned(i), metaData.getPrecision(i),
                            metaData.getScale(i), metaData.getParameterType(i),
                            metaData.getParameterTypeName(i),
                            metaData.getParameterClassName(i), "?" + i));
        }
        return params;
    }

    protected static Signature signature(ResultSetMetaData metaData,
                                         ParameterMetaData parameterMetaData, String sql,
                                         Meta.StatementType statementType) throws SQLException {
        final CursorFactory cf = CursorFactory.LIST;  // because JdbcResultSet#frame
        return new Signature(columns(metaData), sql, parameters(parameterMetaData),
                null, cf, statementType);
    }

    protected static Signature signature(ResultSetMetaData metaData)
            throws SQLException {
        return signature(metaData, null, null, null);
    }

    @Override
    public Map<DatabaseProperty, Object> getDatabaseProperties(ConnectionHandle ch) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final Map<DatabaseProperty, Object> map = new HashMap<>();
            final Connection conn = getConnection(ch.id);
            final DatabaseMetaData metaData = conn.getMetaData();
            for (DatabaseProperty p : DatabaseProperty.values()) {
                addProperty(map, metaData, p);
            }
            return map;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object addProperty(Map<DatabaseProperty, Object> map,
                                      DatabaseMetaData metaData, DatabaseProperty p) throws SQLException {
        Object propertyValue;
        if (p.isJdbc) {
            try {
                propertyValue = p.method.invoke(metaData);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        } else {
            propertyValue = p.defaultValue;
        }

        return map.put(p, propertyValue);
    }

    @Override
    public MetaResultSet getTables(ConnectionHandle ch, String catalog, Pat schemaPattern,
                                   Pat tableNamePattern, List<String> typeList) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final ResultSet rs =
                    getConnection(ch.id).getMetaData().getTables(catalog, schemaPattern.s,
                            tableNamePattern.s, toArray(typeList));
            int stmtId = registerMetaStatement(rs);
            return JdbcResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Registers a StatementInfo for the given ResultSet, returning the id under
     * which it is registered. This should be used for metadata ResultSets, which
     * have an implicit statement created.
     */
    private int registerMetaStatement(ResultSet rs) throws SQLException {
        final int id = statementIdGenerator.getAndIncrement();
        StatementInfo statementInfo = new StatementInfo(rs.getStatement());
        statementInfo.setResultSet(rs);
        statementCache.put(id, statementInfo);
        return id;
    }

    @Override
    public MetaResultSet getColumns(ConnectionHandle ch, String catalog, Pat schemaPattern,
                                    Pat tableNamePattern, Pat columnNamePattern) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final ResultSet rs =
                    getConnection(ch.id).getMetaData().getColumns(catalog, schemaPattern.s,
                            tableNamePattern.s, columnNamePattern.s);
            int stmtId = registerMetaStatement(rs);
            return JdbcResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final ResultSet rs =
                    getConnection(ch.id).getMetaData().getSchemas(catalog, schemaPattern.s);
            int stmtId = registerMetaStatement(rs);
            return JdbcResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getCatalogs(ConnectionHandle ch) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final ResultSet rs = getConnection(ch.id).getMetaData().getCatalogs();
            int stmtId = registerMetaStatement(rs);
            return JdbcResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getTableTypes(ConnectionHandle ch) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final ResultSet rs = getConnection(ch.id).getMetaData().getTableTypes();
            int stmtId = registerMetaStatement(rs);
            return JdbcResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getProcedures(ConnectionHandle ch, String catalog, Pat schemaPattern,
                                       Pat procedureNamePattern) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final ResultSet rs =
                    getConnection(ch.id).getMetaData().getProcedures(catalog, schemaPattern.s,
                            procedureNamePattern.s);
            int stmtId = registerMetaStatement(rs);
            return JdbcResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getProcedureColumns(ConnectionHandle ch, String catalog, Pat schemaPattern,
                                             Pat procedureNamePattern, Pat columnNamePattern) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final ResultSet rs =
                    getConnection(ch.id).getMetaData().getProcedureColumns(catalog,
                            schemaPattern.s, procedureNamePattern.s, columnNamePattern.s);
            int stmtId = registerMetaStatement(rs);
            return JdbcResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getColumnPrivileges(ConnectionHandle ch, String catalog, String schema,
                                             String table, Pat columnNamePattern) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final ResultSet rs =
                    getConnection(ch.id).getMetaData().getColumnPrivileges(catalog, schema,
                            table, columnNamePattern.s);
            int stmtId = registerMetaStatement(rs);
            return JdbcResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getTablePrivileges(ConnectionHandle ch, String catalog, Pat schemaPattern,
                                            Pat tableNamePattern) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final ResultSet rs =
                    getConnection(ch.id).getMetaData().getTablePrivileges(catalog,
                            schemaPattern.s, tableNamePattern.s);
            int stmtId = registerMetaStatement(rs);
            return JdbcResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getBestRowIdentifier(ConnectionHandle ch, String catalog, String schema,
                                              String table, int scope, boolean nullable) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        LOG.trace("getBestRowIdentifier catalog:{} schema:{} table:{} scope:{} nullable:{}", catalog,
                schema, table, scope, nullable);
        try {
            final ResultSet rs =
                    getConnection(ch.id).getMetaData().getBestRowIdentifier(catalog, schema,
                            table, scope, nullable);
            int stmtId = registerMetaStatement(rs);
            return JdbcResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getVersionColumns(ConnectionHandle ch, String catalog, String schema,
                                           String table) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        LOG.trace("getVersionColumns catalog:{} schema:{} table:{}", catalog, schema, table);
        try {
            final ResultSet rs =
                    getConnection(ch.id).getMetaData().getVersionColumns(catalog, schema, table);
            int stmtId = registerMetaStatement(rs);
            return JdbcResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getPrimaryKeys(ConnectionHandle ch, String catalog, String schema,
                                        String table) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        LOG.trace("getPrimaryKeys catalog:{} schema:{} table:{}", catalog, schema, table);
        try {
            final ResultSet rs =
                    getConnection(ch.id).getMetaData().getPrimaryKeys(catalog, schema, table);
            int stmtId = registerMetaStatement(rs);
            return JdbcResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getImportedKeys(ConnectionHandle ch, String catalog, String schema,
                                         String table) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public MetaResultSet getExportedKeys(ConnectionHandle ch, String catalog, String schema,
                                         String table) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public MetaResultSet getCrossReference(ConnectionHandle ch, String parentCatalog,
                                           String parentSchema, String parentTable, String foreignCatalog,
                                           String foreignSchema, String foreignTable) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public MetaResultSet getTypeInfo(ConnectionHandle ch) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final ResultSet rs = getConnection(ch.id).getMetaData().getTypeInfo();
            int stmtId = registerMetaStatement(rs);
            return JdbcResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getIndexInfo(ConnectionHandle ch, String catalog, String schema,
                                      String table, boolean unique, boolean approximate) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public MetaResultSet getUDTs(ConnectionHandle ch, String catalog, Pat schemaPattern,
                                 Pat typeNamePattern, int[] types) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public MetaResultSet getSuperTypes(ConnectionHandle ch, String catalog, Pat schemaPattern,
                                       Pat typeNamePattern) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public MetaResultSet getSuperTables(ConnectionHandle ch, String catalog, Pat schemaPattern,
                                        Pat tableNamePattern) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public MetaResultSet getAttributes(ConnectionHandle ch, String catalog, Pat schemaPattern,
                                       Pat typeNamePattern, Pat attributeNamePattern) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public MetaResultSet getClientInfoProperties(ConnectionHandle ch) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public MetaResultSet getFunctions(ConnectionHandle ch, String catalog, Pat schemaPattern,
                                      Pat functionNamePattern) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public MetaResultSet getFunctionColumns(ConnectionHandle ch, String catalog, Pat schemaPattern,
                                            Pat functionNamePattern, Pat columnNamePattern) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public MetaResultSet getPseudoColumns(ConnectionHandle ch, String catalog, Pat schemaPattern,
                                          Pat tableNamePattern, Pat columnNamePattern) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public Iterable<Object> createIterable(StatementHandle handle, QueryState state,
                                           Signature signature, List<TypedValue> parameterValues, Frame firstFrame) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    protected Connection getConnection(String id) throws SQLException {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        if (id == null) {
            throw new NullPointerException("Connection id is null.");
        }
        Connection conn = connectionCache.getIfPresent(id);
        if (conn == null) {
            throw new NoSuchConnectionException("Connection not found: invalid id, closed, or expired: "
                    + id);
        }
        return conn;
    }

    @Override
    public StatementHandle createStatement(ConnectionHandle ch) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final Connection conn = getConnection(ch.id);
            final Statement statement = conn.createStatement();
            final int id = statementIdGenerator.getAndIncrement();
            statementCache.put(id, new StatementInfo(statement));
            StatementHandle h = new StatementHandle(ch.id, id, null);
            LOG.trace("created statement {}", h);
            return h;
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    @Override
    public void closeStatement(StatementHandle h) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        StatementInfo info = statementCache.getIfPresent(h.id);
        if (info == null || info.statement == null) {
            LOG.debug("client requested close unknown statement {}", h);
            return;
        }
        LOG.trace("closing statement {}", h);
        try {
            ResultSet results = info.getResultSet();
            if (info.isResultSetInitialized() && null != results) {
                results.close();
            }
            info.statement.close();
        } catch (SQLException e) {
            throw propagate(e);
        } finally {
            statementCache.invalidate(h.id);
        }
    }

    @Override
    public void openConnection(ConnectionHandle ch,
                               Map<String, String> info) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        final Properties fullInfo = new Properties();
        fullInfo.putAll(this.info);
        if (info != null) {
            fullInfo.putAll(info);
        }

        synchronized (this) {
            try {
                if (connectionCache.asMap().containsKey(ch.id)) {
                    throw new RuntimeException("Connection already exists: " + ch.id);
                }

                // 校验用户
                final String username = fullInfo.getProperty("user");
                final String password = fullInfo.getProperty("password");
                // 校验管理员用户
                String[] superusers = PhoenixClientConfig.HBASE_SUPERUSER.split(",");
                String superpassword = PhoenixClientConfig.PHOENIX_CLIENT_HBASE_SUPERUSER_PASSWORD;
                boolean isSuperuser = false;
                for (String superuser : superusers) {
                    if (superuser.equals(username) && superpassword.equals(password)) {
                        isSuperuser = true;
                    }
                }

                if (!isSuperuser) {
                    // 校验普通用户
                    List<Map<String, Object>> users = JdbcUtil.executeQuery(
                            new JdbcUtil.Sql("SELECT USERNAME, PASSWORD FROM SYSTEM.USER WHERE USERNAME = ? AND PASSWORD = ?")
                                    .addParams(username, password));
                    if (users.isEmpty()) {
                        throw new SQLException("创建连接失败，不存在该用户或密码不正确！username = " + username);
                    }
                }

                // 创建连接
                UserGroupInformation currentUser = UserGroupInformation.createRemoteUser(fullInfo.getProperty("user"));
                Object result = currentUser.doAs((PrivilegedAction<Object>) () -> {
                    try {
                        return DriverManager.getConnection(url, fullInfo);
                    } catch (SQLException throwables) {
                        return throwables;
                    }
                });

                if (result instanceof Connection) {
                    connectionCache.put(ch.id, (Connection) result);
                    userCache.put(ch.id, username);
                } else {
                    throw (SQLException) result;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void closeConnection(ConnectionHandle ch) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        Connection conn = connectionCache.getIfPresent(ch.id);
        if (conn == null) {
            LOG.debug("client requested close unknown connection {}", ch);
            return;
        }
        LOG.trace("closing connection {}", ch);
        try {
            conn.close();
        } catch (SQLException e) {
            throw propagate(e);
        } finally {
            connectionCache.invalidate(ch.id);
            userCache.invalidate(ch.id);
        }
    }

    protected void apply(Connection conn, ConnectionProperties connProps)
            throws SQLException {
        if (connProps.isAutoCommit() != null) {
            conn.setAutoCommit(connProps.isAutoCommit());
        }
        if (connProps.isReadOnly() != null) {
            conn.setReadOnly(connProps.isReadOnly());
        }
        if (connProps.getTransactionIsolation() != null) {
            conn.setTransactionIsolation(connProps.getTransactionIsolation());
        }
        if (connProps.getCatalog() != null) {
            conn.setCatalog(connProps.getCatalog());
        }
        if (connProps.getSchema() != null) {
            conn.setSchema(connProps.getSchema());
        }
    }

    @Override
    public ConnectionProperties connectionSync(ConnectionHandle ch,
                                               ConnectionProperties connProps) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        LOG.trace("syncing properties for connection {}", ch);
        try {
            Connection conn = getConnection(ch.id);
            ConnectionPropertiesImpl props = new ConnectionPropertiesImpl(conn).merge(connProps);
            if (props.isDirty()) {
                apply(conn, props);
                props.setDirty(false);
            }
            return props;
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    RuntimeException propagate(Throwable e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        } else if (e instanceof Error) {
            throw (Error) e;
        } else {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StatementHandle prepare(ConnectionHandle ch, String sql,
                                   long maxRowCount) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final Connection conn = getConnection(ch.id);
            final PreparedStatement statement = conn.prepareStatement(sql);
            final int id = getStatementIdGenerator().getAndIncrement();
            Meta.StatementType statementType = null;
            if (statement.isWrapperFor(AvaticaPreparedStatement.class)) {
                final AvaticaPreparedStatement avaticaPreparedStatement;
                avaticaPreparedStatement =
                        statement.unwrap(AvaticaPreparedStatement.class);
                statementType = avaticaPreparedStatement.getStatementType();
            }
            // Set the maximum number of rows
            setMaxRows(statement, maxRowCount);
            getStatementCache().put(id, new StatementInfo(statement));
            StatementHandle h = new StatementHandle(ch.id, id,
                    signature(statement.getMetaData(), statement.getParameterMetaData(),
                            sql, statementType));
            LOG.trace("prepared statement {}", h);
            return h;
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public ExecuteResult prepareAndExecute(StatementHandle h, String sql,
                                           long maxRowCount, PrepareCallback callback) throws NoSuchStatementException {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return prepareAndExecute(h, sql, maxRowCount, AvaticaUtils.toSaturatedInt(maxRowCount),
                callback);
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount,
                                           int maxRowsInFirstFrame, PrepareCallback callback) throws NoSuchStatementException {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            // 查询缓存
            ExecuteResult cache = getSelectCache(sql, null);
            if (cache != null) {
                return cache;
            }

            final StatementInfo info = getStatementCache().getIfPresent(h.id);
            if (info == null) {
                throw new NoSuchStatementException(h);
            }
            final Statement statement = info.statement;
            // Make sure that we limit the number of rows for the query
            setMaxRows(statement, maxRowCount);
            boolean ret = statement.execute(sql);
            info.setResultSet(statement.getResultSet());
            // Either execute(sql) returned true or the resultSet was null
            assert ret || null == info.getResultSet();
            final List<MetaResultSet> resultSets = new ArrayList<>();
            if (null == info.getResultSet()) {
                // Create a special result set that just carries update count
                resultSets.add(
                        JdbcResultSet.count(h.connectionId, h.id,
                                AvaticaUtils.getLargeUpdateCount(statement)));
            } else {
                resultSets.add(
                        JdbcResultSet.create(h.connectionId, h.id, info.getResultSet(), maxRowsInFirstFrame));
            }
            LOG.trace("prepAndExec statement {}", h);

            // TODO: review client to ensure statementId is updated when appropriate
            ExecuteResult result = new ExecuteResult(resultSets);

            // 执行完成后进行缓存等操作
            afterSuccess(sql, null, result);

            // 执行成功后写入审计日志
            writeAudit(System.currentTimeMillis(), userCache.getIfPresent(h.connectionId),
                    sql, null, true, null);

            return result;
        } catch (SQLException e) {
            // 执行失败后写入审计日志
            writeAudit(System.currentTimeMillis(), userCache.getIfPresent(h.connectionId),
                    sql, null, false, e);
            throw propagate(e);
        }
    }

    /**
     * Sets the provided maximum number of rows on the given statement.
     *
     * @param statement   The JDBC Statement to operate on
     * @param maxRowCount The maximum number of rows which should be returned for the query
     */
    void setMaxRows(Statement statement, long maxRowCount) throws SQLException {
        // Special handling of maxRowCount as JDBC 0 is unlimited, our meta 0 row
        if (maxRowCount > 0) {
            AvaticaUtils.setLargeMaxRows(statement, maxRowCount);
        } else if (maxRowCount < 0) {
            statement.setMaxRows(0);
        }
    }

    @Override
    public boolean syncResults(StatementHandle sh, QueryState state, long offset)
            throws NoSuchStatementException {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final Connection conn = getConnection(sh.connectionId);
            final StatementInfo info = statementCache.getIfPresent(sh.id);
            if (null == info) {
                throw new NoSuchStatementException(sh);
            }
            final Statement statement = info.statement;
            // Let the state recreate the necessary ResultSet on the Statement
            info.setResultSet(state.invoke(conn, statement));

            if (null != info.getResultSet()) {
                // If it is non-null, try to advance to the requested offset.
                return info.advanceResultSetToOffset(info.getResultSet(), offset);
            }

            // No results, nothing to do. Client can move on.
            return false;
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    @Override
    public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount) throws
            NoSuchStatementException, MissingResultsException {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        LOG.trace("fetching {} offset:{} fetchMaxRowCount:{}", h, offset, fetchMaxRowCount);
        try {
            final StatementInfo statementInfo = statementCache.getIfPresent(h.id);
            if (null == statementInfo) {
                // Statement might have expired, or never existed on this server.
                throw new NoSuchStatementException(h);
            }

            if (!statementInfo.isResultSetInitialized()) {
                // The Statement exists, but the results are missing. Need to call syncResults(...)
                throw new MissingResultsException(h);
            }
            if (statementInfo.getResultSet() == null) {
                return Frame.EMPTY;
            } else {
                return JdbcResultSet.frame(statementInfo, statementInfo.getResultSet(), offset,
                        fetchMaxRowCount, calendar, Optional.<Meta.Signature>absent());
            }
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    private static String[] toArray(List<String> typeList) {
        if (typeList == null) {
            return null;
        }
        return typeList.toArray(new String[typeList.size()]);
    }

    /**
     * 解析SQL获取BindableStatement
     *
     * @param sql
     * @return
     */
    private BindableStatement getStatement(String sql) {
        try {
            return new SQLParser(sql).parseStatement();
        } catch (SQLException throwables) {
            return null;
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues,
                                 long maxRowCount) throws NoSuchStatementException {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return execute(h, parameterValues, AvaticaUtils.toSaturatedInt(maxRowCount));
    }

    @Override
    public ExecuteResult execute(StatementHandle h,
                                 List<TypedValue> parameterValues, int maxRowsInFirstFrame) throws NoSuchStatementException {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        String sql = null;
        try {
            if (MetaImpl.checkParameterValueHasNull(parameterValues)) {
                throw new SQLException("exception while executing query: unbound parameter");
            }

            final StatementInfo statementInfo = statementCache.getIfPresent(h.id);
            if (null == statementInfo) {
                throw new NoSuchStatementException(h);
            }
            final List<MetaResultSet> resultSets;
            final PreparedStatement preparedStatement =
                    (PreparedStatement) statementInfo.statement;

            // 查询缓存
            sql = preparedStatement.toString();
            ExecuteResult cache = getSelectCache(sql, parameterValues);
            if (cache != null) {
                return cache;
            }

            if (parameterValues != null) {
                for (int i = 0; i < parameterValues.size(); i++) {
                    TypedValue o = parameterValues.get(i);
                    preparedStatement.setObject(i + 1, o.toJdbc(calendar));
                }
            }

            if (preparedStatement.execute()) {
                final Signature signature2;
                if (preparedStatement.isWrapperFor(AvaticaPreparedStatement.class)) {
                    signature2 = h.signature;
                } else {
                    h.signature = signature(preparedStatement.getMetaData(),
                            preparedStatement.getParameterMetaData(), h.signature.sql,
                            Meta.StatementType.SELECT);
                    signature2 = h.signature;
                }

                // Make sure we set this for subsequent fetch()'s to find the result set.
                statementInfo.setResultSet(preparedStatement.getResultSet());

                if (statementInfo.getResultSet() == null) {
                    resultSets = Collections.<MetaResultSet>singletonList(
                            JdbcResultSet.empty(h.connectionId, h.id, signature2));
                } else {
                    resultSets = Collections.<MetaResultSet>singletonList(
                            JdbcResultSet.create(h.connectionId, h.id, statementInfo.getResultSet(),
                                    maxRowsInFirstFrame, signature2));
                }
            } else {
                resultSets = Collections.<MetaResultSet>singletonList(
                        JdbcResultSet.count(h.connectionId, h.id, preparedStatement.getUpdateCount()));
            }

            ExecuteResult result = new ExecuteResult(resultSets);

            // 执行完成后进行缓存等操作
            afterSuccess(sql, parameterValues, result);

            // 执行成功后写入审计日志
            writeAudit(System.currentTimeMillis(), userCache.getIfPresent(h.connectionId),
                    sql, parameterValues, true, null);

            return result;
        } catch (SQLException e) {
            // 执行失败后写入审计日志
            writeAudit(System.currentTimeMillis(), userCache.getIfPresent(h.connectionId),
                    sql, parameterValues, false, e);
            throw propagate(e);
        }
    }

    @Override
    public void commit(ConnectionHandle ch) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final Connection conn = getConnection(ch.id);
            conn.commit();
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    @Override
    public void rollback(ConnectionHandle ch) {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final Connection conn = getConnection(ch.id);
            conn.rollback();
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    @Override
    public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle h,
                                                     List<String> sqlCommands) throws NoSuchStatementException {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            // Get the statement
            final StatementInfo info = statementCache.getIfPresent(h.id);
            if (info == null) {
                throw new NoSuchStatementException(h);
            }

            // addBatch() for each sql command
            final Statement stmt = info.statement;
            for (String sqlCommand : sqlCommands) {
                stmt.addBatch(sqlCommand);
            }

            // Execute the batch and return the results
            return new ExecuteBatchResult(AvaticaUtils.executeLargeBatch(stmt));
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    @Override
    public ExecuteBatchResult executeBatch(StatementHandle h,
                                           List<List<TypedValue>> updateBatches) throws NoSuchStatementException {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final StatementInfo info = statementCache.getIfPresent(h.id);
            if (null == info) {
                throw new NoSuchStatementException(h);
            }

            final PreparedStatement preparedStmt = (PreparedStatement) info.statement;
            int rowUpdate = 1;
            for (List<TypedValue> batch : updateBatches) {
                int i = 1;
                for (TypedValue value : batch) {
                    // Set the TypedValue in the PreparedStatement
                    try {
                        preparedStmt.setObject(i, value.toJdbc(calendar));
                        i++;
                    } catch (SQLException e) {
                        throw new RuntimeException("Failed to set value on row #" + rowUpdate
                                + " and column #" + i, e);
                    }
                    // Track the update number for better error messages
                    rowUpdate++;
                }
                preparedStmt.addBatch();
            }
            return new ExecuteBatchResult(AvaticaUtils.executeLargeBatch(preparedStmt));
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    @Override
    public ExecuteBatchResult executeBatchProtobuf(StatementHandle h,
                                                   List<Requests.UpdateBatch> updateBatches) throws NoSuchStatementException {
        LOG.debug("正在调用：" + Thread.currentThread().getStackTrace()[1].getMethodName());
        try {
            final StatementInfo info = statementCache.getIfPresent(h.id);
            if (null == info) {
                throw new NoSuchStatementException(h);
            }

            final PreparedStatement preparedStmt = (PreparedStatement) info.statement;
            for (Requests.UpdateBatch update : updateBatches) {
                int i = 1;
                for (Common.TypedValue value : update.getParameterValuesList()) {
                    // Use the value and then increment
                    preparedStmt.setObject(i++, TypedValue.protoToJdbc(value, calendar));
                }
                preparedStmt.addBatch();
            }
            return new ExecuteBatchResult(AvaticaUtils.executeLargeBatch(preparedStmt));
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    /**
     * Configurable statement cache settings.
     */
    public enum StatementCacheSettings {
        /**
         * JDBC connection property for setting connection cache concurrency level.
         */
        CONCURRENCY_LEVEL(STMT_CACHE_KEY_BASE + ".concurrency", "100"),

        /**
         * JDBC connection property for setting connection cache initial capacity.
         */
        INITIAL_CAPACITY(STMT_CACHE_KEY_BASE + ".initialcapacity", "1000"),

        /**
         * JDBC connection property for setting connection cache maximum capacity.
         */
        MAX_CAPACITY(STMT_CACHE_KEY_BASE + ".maxcapacity", "10000"),

        /**
         * JDBC connection property for setting connection cache expiration duration.
         *
         * <p>Used in conjunction with {@link #EXPIRY_UNIT}.</p>
         */
        EXPIRY_DURATION(STMT_CACHE_KEY_BASE + ".expirydiration", "5"),

        /**
         * JDBC connection property for setting connection cache expiration unit.
         *
         * <p>Used in conjunction with {@link #EXPIRY_DURATION}.</p>
         */
        EXPIRY_UNIT(STMT_CACHE_KEY_BASE + ".expiryunit", TimeUnit.MINUTES.name());

        private final String key;
        private final String defaultValue;

        StatementCacheSettings(String key, String defaultValue) {
            this.key = key;
            this.defaultValue = defaultValue;
        }

        /**
         * The configuration key for specifying this setting.
         */
        public String key() {
            return key;
        }

        /**
         * The default value for this setting.
         */
        public String defaultValue() {
            return defaultValue;
        }
    }

    /**
     * Configurable connection cache settings.
     */
    public enum ConnectionCacheSettings {
        /**
         * JDBC connection property for setting connection cache concurrency level.
         */
        CONCURRENCY_LEVEL(CONN_CACHE_KEY_BASE + ".concurrency", "10"),

        /**
         * JDBC connection property for setting connection cache initial capacity.
         */
        INITIAL_CAPACITY(CONN_CACHE_KEY_BASE + ".initialcapacity", "100"),

        /**
         * JDBC connection property for setting connection cache maximum capacity.
         */
        MAX_CAPACITY(CONN_CACHE_KEY_BASE + ".maxcapacity", "1000"),

        /**
         * JDBC connection property for setting connection cache expiration duration.
         */
        EXPIRY_DURATION(CONN_CACHE_KEY_BASE + ".expiryduration", "10"),

        /**
         * JDBC connection property for setting connection cache expiration unit.
         */
        EXPIRY_UNIT(CONN_CACHE_KEY_BASE + ".expiryunit", TimeUnit.MINUTES.name());

        private final String key;
        private final String defaultValue;

        ConnectionCacheSettings(String key, String defaultValue) {
            this.key = key;
            this.defaultValue = defaultValue;
        }

        /**
         * The configuration key for specifying this setting.
         */
        public String key() {
            return key;
        }

        /**
         * The default value for this setting.
         */
        public String defaultValue() {
            return defaultValue;
        }
    }

    /**
     * Callback for {@link #connectionCache} member expiration.
     */
    private class ConnectionExpiryHandler
            implements RemovalListener<String, Connection> {

        @Override
        public void onRemoval(RemovalNotification<String, Connection> notification) {
            String connectionId = notification.getKey();
            Connection doomed = notification.getValue();
            LOG.debug("Expiring connection {} because {}", connectionId, notification.getCause());
            try {
                if (doomed != null) {
                    doomed.close();
                }
            } catch (Throwable t) {
                LOG.info("Exception thrown while expiring connection {}", connectionId, t);
            }
        }
    }

    /**
     * Callback for {@link #statementCache} member expiration.
     */
    private class StatementExpiryHandler
            implements RemovalListener<Integer, StatementInfo> {
        @Override
        public void onRemoval(RemovalNotification<Integer, StatementInfo> notification) {
            Integer stmtId = notification.getKey();
            StatementInfo doomed = notification.getValue();
            if (doomed == null) {
                // log/throw?
                return;
            }
            LOG.debug("Expiring statement {} because {}", stmtId, notification.getCause());
            try {
                if (doomed.getResultSet() != null) {
                    doomed.getResultSet().close();
                }
                if (doomed.statement != null) {
                    doomed.statement.close();
                }
            } catch (Throwable t) {
                LOG.info("Exception thrown while expiring statement {}", stmtId, t);
            }
        }
    }

    private class UserExpiryHandler implements RemovalListener<String, String> {
        @Override
        public void onRemoval(RemovalNotification<String, String> notification) {
            String user = notification.getValue();

            LOG.debug("用户{}已关闭Connection！", user);
        }
    }

    /**
     * 检查并写入缓存
     *
     * @param sql
     * @param parameterValues
     * @param result
     * @throws SQLException
     */
    private void afterSuccess(String sql, List<TypedValue> parameterValues, Meta.ExecuteResult result) throws SQLException {
        BindableStatement stmt = new SQLParser(sql).parseStatement();
        long current = System.currentTimeMillis();
        if (stmt != null) {
            // 如果是SELECT命令，则写入查询缓存
            if (stmt instanceof SelectStatement) {
                queryCache.put(new QueryCache.CacheKey((SelectStatement) stmt, parameterValues),
                        new QueryCache.CacheValue(result, current));
            }

            // 如果是UPSERT和DELETE命令，则写入表更新缓存
            else if (stmt instanceof UpsertStatement || stmt instanceof DeleteStatement) {
                tableUpdateTimeCache.put(((DMLStatement) stmt).getTable().getName().toString(), current);
            }

            // 如果是DROP命令，则删除表更新缓存
            else if (stmt instanceof DropTableStatement) {
                tableUpdateTimeCache.remove(((DropTableStatement) stmt).getTableName().toString());
            }

            // 如果是CREATE INDEX ... ASYNC命令，则自动开始构建索引
            else if (stmt instanceof CreateIndexStatement && ((CreateIndexStatement) stmt).isAsync()) {
                asyncIndex.create(stmt);
            }
        }
    }


    /**
     * 写入审计日志
     *
     * @param time
     * @param user
     * @param sql
     * @param parameterValues
     * @param success
     * @param e
     */
    private void writeAudit(long time, String user, String sql,
                            List<TypedValue> parameterValues, boolean success, Exception e) {
        if (auditCache.isEnable()) {
            for (String pre : auditCache.getPres()) {
                if (e != null || sql.toUpperCase().startsWith(pre) || "*".equals(pre)) {
                    List<Object> params = new ArrayList<>();
                    if (parameterValues != null) {
                        for (TypedValue parameterValue : parameterValues) {
                            params.add(parameterValue.value);
                        }
                    }

                    StringBuilder errorMsg = new StringBuilder();
                    if (e != null) {
                        errorMsg.append(e.toString());
                        StackTraceElement[] trace = e.getStackTrace();
                        for (StackTraceElement traceElement : trace) {
                            errorMsg.append("\tat ").append(traceElement);
                        }
                    }

                    auditCache.put(new AuditCache.Key(time, user, sql, params, success, errorMsg.toString()), 1);
                    break;
                }
            }
        }
    }

    /**
     * 获取查询缓存
     *
     * @param sql
     * @param parameterValues
     * @return
     */
    private Meta.ExecuteResult getSelectCache(String sql, List<TypedValue> parameterValues) throws SQLException {
        BindableStatement stmt = new SQLParser(sql).parseStatement();
        if (stmt instanceof SelectStatement) {
            QueryCache.CacheValue value = queryCache.get(new QueryCache.CacheKey((SelectStatement) stmt, parameterValues));
            if (value != null) {
                LOG.info("检测到有效查询缓存，返回缓存结果！");
                return value.getResult();
            }
        }
        return null;
    }
}

// End JdbcMeta.java
