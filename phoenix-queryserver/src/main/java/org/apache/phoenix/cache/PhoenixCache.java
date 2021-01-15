package org.apache.phoenix.cache;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author colar
 * @date 2021-01-19 下午6:10
 */
public abstract class PhoenixCache<K, V> {
    protected final CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder().build(true);
    protected Cache<K, V> cache;

    public PhoenixCache() {
        this.cache = createCache();
    }

    /**
     * 创建缓存
     *
     * @return
     */
    protected abstract Cache<K, V> createCache();

    /**
     * 写入缓存
     *
     * @param k
     * @param v
     */
    public void put(K k, V v) {
        cache.put(k, v);
    }

    /**
     * 写入缓存
     *
     * @param map
     */
    public void putAll(Map<K, V> map) {
        cache.putAll(map);
    }

    /**
     * 读取缓存
     *
     * @param k
     * @return
     */
    public V get(K k) {
        return cache.get(k);
    }

    /**
     * 读取缓存
     *
     * @param set
     * @return
     */
    public Map<K, V> getAll(Set<K> set) {
        return cache.getAll(set);
    }

    /**
     * 删除缓存
     *
     * @param k
     */
    public void remove(K k) {
        cache.remove(k);
    }

    /**
     * 删除缓存
     *
     * @param ks
     */
    public void removeAll(Set<K> ks) {
        cache.removeAll(ks);
    }

    /**
     * 返回缓存迭代器
     *
     * @return
     */
    public Iterator<Cache.Entry<K, V>> iterator() {
        return cache.iterator();
    }
}
