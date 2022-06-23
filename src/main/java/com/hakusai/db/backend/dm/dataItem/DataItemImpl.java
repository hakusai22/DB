package com.hakusai.db.backend.dm.dataItem;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.hakusai.db.backend.dm.page.Page;
import com.hakusai.db.backend.common.SubArray;
import com.hakusai.db.backend.dm.DataManagerImpl;

/**
 * 保存一个 dm 的引用是因为其释放依赖 dm 的释放（dm 同时实现了缓存接口，用于缓存 DataItem），以及修改数据时落日志
 *
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 *
 * 其中 ValidFlag 占用 1 字节，标识了该 DataItem 是否有效。
 * 删除一个 DataItem，只需要简单地将其有效位设置为 0。
 * DataSize 占用 2 字节，标识了后面 Data 的长度。
 *
 * 上层模块在获取到 DataItem 后，可以通过 data() 方法，
 * 该方法返回的数组是数据共享的，而不是拷贝实现的，所以使用了 SubArray。
 */
public class DataItemImpl implements DataItem {

    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    private DataManagerImpl dm;
    private long uid;
    private Page pg;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    public boolean isValid() {
        return raw.raw[raw.start+OF_VALID] == (byte)0;
    }

    /**
     * 上层模块在获取到 DataItem 后，可以通过 data() 方法，
     * 该方法返回的数组是数据共享的，而不是拷贝实现的，所以使用了 SubArray。
     * @return
     */
    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start+OF_DATA, raw.end);
    }

    /**
     * 在上层模块试图对 DataItem 进行修改时，需要遵循一定的流程：
     * 在修改之前需要调用 before() 方法，
     * 想要撤销修改时，调用 unBefore() 方法，
     * 在修改完成后，调用 after() 方法。整个流程，
     * 主要是为了保存前相数据，并及时落日志。
     * DM 会保证对 DataItem 的修改是原子性的。
     */
    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    //after() 方法，主要就是调用 dm 中的一个方法，对修改操作落日志。
    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    /**
     * 在使用完 DataItem 后，也应当及时调用 release() 方法，释放掉 DataItem 的缓存（由 DM 缓存 DataItem）。
     */
    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
    
}
