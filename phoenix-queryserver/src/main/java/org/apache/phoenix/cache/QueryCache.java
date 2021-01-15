package org.apache.phoenix.cache;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.phoenix.config.PhoenixClientConfig;
import org.apache.phoenix.parse.ConcreteTableNode;
import org.apache.phoenix.parse.DerivedTableNode;
import org.apache.phoenix.parse.InParseNode;
import org.apache.phoenix.parse.JoinTableNode;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.SubqueryParseNode;
import org.apache.phoenix.parse.TableNode;
import org.ehcache.Cache;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author colar
 * @date 2021-01-18 上午10:22
 */
public class QueryCache extends PhoenixCache<QueryCache.CacheKey, QueryCache.CacheValue> {
    private static final QueryCache instance = new QueryCache();
    private final TableUpdateTimeCache tableUpdateTimeCache = TableUpdateTimeCache.getInstance();

    private QueryCache() {
    }

    public static QueryCache getInstance() {
        return instance;
    }

    @Override
    protected Cache<CacheKey, CacheValue> createCache() {
        Long heapSize = PhoenixClientConfig.PHOENIX_CLIENT_QUERY_CACHE_HEAP_SIZE;
        Long idleTime = PhoenixClientConfig.PHOENIX_CLIENT_QUERY_CACHE_IDLE_TIME;

        ResourcePools pools = ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(heapSize, MemoryUnit.B)
                .build();

        CacheConfiguration<CacheKey, CacheValue> conf = CacheConfigurationBuilder
                .newCacheConfigurationBuilder(CacheKey.class, CacheValue.class, pools)
                .withSizeOfMaxObjectGraph(Integer.MAX_VALUE)
                .withExpiry(ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(idleTime)))
                .build();
        return manager.createCache("select", conf);
    }

    /**
     * 写入缓存
     * <p>
     * 系统表关联的查询不写入缓存
     *
     * @param key
     * @param cacheValue
     */
    @Override
    public void put(CacheKey key, CacheValue cacheValue) {
        List<String> tables = getTables(key.getStmt());
        for (String table : tables) {
            if (table.startsWith("SYSTEM.")) {
                return;
            }
        }
        super.put(key, cacheValue);
    }

    /**
     * 读取缓存
     * <p>
     * 读取后进行校验，如果缓存已经过期则返回null
     *
     * @param key
     * @return
     */
    @Override
    public CacheValue get(CacheKey key) {
        CacheValue cacheValue = cache.get(key);

        if (cacheValue != null) {
            List<String> tables = getTables(key.getStmt());
            for (String table : tables) {
                if (tableUpdateTimeCache.get(table) != null && tableUpdateTimeCache.get(table) > cacheValue.getUpdateTime()) {
                    remove(key);
                    return null;
                }
            }
            return cacheValue;
        } else {
            return null;
        }
    }

    /**
     * 根据SelectStatement获取所有查询关联的表
     *
     * @param stmt
     * @return
     */
    private static List<String> getTables(SelectStatement stmt) {
        List<String> tables = new ArrayList<>();
        getTables(tables, stmt);
        return tables;
    }

    private static void getTables(List<String> tables, SelectStatement stmt) {
        for (SelectStatement select : stmt.getSelects()) {
            getTables(tables, select);
        }

        TableNode from = stmt.getFrom();
        getTables(tables, from);

        if (stmt.getWhere() != null) {
            getTables(tables, stmt.getWhere());
        }
    }

    private static void getTables(List<String> tables, TableNode node) {
        if (node instanceof ConcreteTableNode) {
            tables.add(((NamedTableNode) node).getName().toString());
        } else if (node instanceof DerivedTableNode) {
            getTables(tables, ((DerivedTableNode) node).getSelect());
        } else if (node instanceof JoinTableNode) {
            TableNode lhs = ((JoinTableNode) node).getLHS();
            getTables(tables, lhs);
            TableNode rhs = ((JoinTableNode) node).getRHS();
            getTables(tables, rhs);
        }
    }

    private static void getTables(List<String> tables, ParseNode node) {
        if (node instanceof InParseNode) {
            for (ParseNode child : node.getChildren()) {
                if (child instanceof SubqueryParseNode) {
                    getTables(tables, ((SubqueryParseNode) child).getSelectNode());
                }
            }
        }
    }

    /**
     * Select缓存的Key
     */
    public static class CacheKey {
        SelectStatement stmt;
        List<TypedValue> parameterValues;

        public CacheKey(SelectStatement stmt, List<TypedValue> parameterValues) {
            this.stmt = stmt;
            this.parameterValues = parameterValues;
        }

        public SelectStatement getStmt() {
            return stmt;
        }

        public void setStmt(SelectStatement stmt) {
            this.stmt = stmt;
        }

        public List<TypedValue> getParameterValues() {
            return parameterValues;
        }

        public void setParameterValues(List<TypedValue> parameterValues) {
            this.parameterValues = parameterValues;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(stmt, cacheKey.stmt) &&
                    Objects.equals(parameterValues, cacheKey.parameterValues);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stmt, parameterValues);
        }
    }

    /**
     * Select缓存的Value
     */
    public static class CacheValue {
        Meta.ExecuteResult result;
        Long updateTime;

        public CacheValue(Meta.ExecuteResult result, Long updateTime) {
            this.result = result;
            this.updateTime = updateTime;
        }

        public Meta.ExecuteResult getResult() {
            return result;
        }

        public void setResult(Meta.ExecuteResult result) {
            this.result = result;
        }

        public Long getUpdateTime() {
            return updateTime;
        }

        public void setUpdateTime(Long updateTime) {
            this.updateTime = updateTime;
        }
    }
}
