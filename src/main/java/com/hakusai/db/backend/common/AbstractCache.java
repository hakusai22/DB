package com.hakusai.db.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.hakusai.db.common.Error;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 * 问题的根源还是，LRU 策略中，资源驱逐不可控，上层模块无法感知。
 * 而引用计数策略正好解决了这个问题，只有上层模块主动释放引用，缓存在确保没有模块在使用这个资源了，才会去驱逐资源。
 */
public abstract class AbstractCache<T> {
    /**
     * 引用计数嘛，除了普通的缓存功能，还需要另外维护一个计数。
     * 除此以外，为了应对多线程场景，还需要记录哪些资源正在从数据源获取中（从数据源获取资源是一个相对费时的操作）
     */
    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 元素的引用个数
    private HashMap<Long, Boolean> getting;             // 正在获取某资源的线程

    private int maxResource;                            // 缓存的最大缓存资源数
    private int count = 0;                              // 缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 于是，在通过 get() 方法获取资源时，首先进入一个死循环，来无限尝试从缓存里获取。
     * 首先就需要检查这个时候是否有其他线程正在从数据源获取这个资源，如果有，就过会再来看看（
     * @param key
     * @return
     * @throws Exception
     */
    protected T get(long key) throws Exception {
        while(true) {
            lock.lock();
            if(getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            /**
             * 当然如果资源在缓存中，就可以直接获取并返回了，记得要给资源的引用数 +1。
             * 否则，如果缓存没满的话，就在 getting 中注册一下，该线程准备从数据源获取资源了。
             */
            if(cache.containsKey(key)) {
                // 资源在缓存中，直接返回
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            // 尝试获取该资源
            if(maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            count ++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        /**
         * 从数据源获取资源就比较简单了，直接调用那个抽象方法即可，获取完成记得从 getting 中删除 key。
         */
        T obj = null;
        try {
            obj = getForCache(key);
        } catch(Exception e) {
            lock.lock();
            count --;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();
        
        return obj;
    }

    /**
     * 强行释放一个缓存
     * 增加了一个方法 release(key)，用于在上册模块不使用某个资源时，释放对资源的引用。
     * 当引用归零时，缓存就会驱逐这个资源。
     * 同样，在缓存满了之后，引用计数法无法自动释放缓存，此时应该直接报错（和 JVM 似的，直接 OOM）
     *
     * 释放一个缓存就简单多了，直接从 references 中减 1，
     * 如果已经减到 0 了，就可以回源，并且删除缓存中所有相关的结构了：
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key)-1;
            if(ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count --;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 缓存应当还有以一个安全关闭的功能，在关闭时，需要将缓存中所有的资源强行回源。
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                release(key);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }




    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
