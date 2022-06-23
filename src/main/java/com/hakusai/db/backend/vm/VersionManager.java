package com.hakusai.db.backend.vm;

import com.hakusai.db.backend.tm.TransactionManager;
import com.hakusai.db.backend.dm.DataManager;

/**
 * VM 基于两段锁协议实现了调度序列的可串行化，并实现了 MVCC 以消除读写阻塞。同时实现了两种隔离级别。
 *
 * 类似于 Data Manager 是 MYDB 的数据管理核心，Version Manager 是 MYDB 的事务和数据版本的管理核心。
 *
 * MYDB 通过 MVCC，降低了事务的阻塞概率。
 * 譬如，T1 想要更新记录 X 的值，于是 T1 需要首先获取 X 的锁，接着更新，也就是创建了一个新的 X 的版本，假设为 x3。
 * 假设 T1 还没有释放 X 的锁时，T2 想要读取 X 的值，这时候就不会阻塞，MYDB 会返回一个较老版本的 X，例如 x2。
 * 这样最后执行的结果，就等价于，T2 先执行，T1 后执行，调度序列依然是可串行化的。
 * 如果 X 没有一个更老的版本，那只能等待 T1 释放锁了。所以只是降低了概率。
 *
 * 规定1：正在进行的事务，不会读取其他任何未提交的事务产生的数据。
 * 规定2：正在进行的事务，不会修改其他任何未提交的事务修改或产生的数据。
 */

public interface VersionManager {
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }

}
