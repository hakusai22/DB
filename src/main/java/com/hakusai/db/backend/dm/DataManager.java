package com.hakusai.db.backend.dm;

import com.hakusai.db.backend.dm.dataItem.DataItem;
import com.hakusai.db.backend.dm.logger.Logger;
import com.hakusai.db.backend.dm.page.PageOne;
import com.hakusai.db.backend.dm.pageCache.PageCache;
import com.hakusai.db.backend.tm.TransactionManager;

/**
 * DM 直接管理数据库 DB 文件和日志文件。DM 的主要职责有：
 * 1) 分页管理 DB 文件，并进行缓存；
 * 2) 管理日志文件，保证在发生错误时可以根据日志进行恢复；
 * 3) 抽象 DB 文件为 DataItem 供上层模块使用，并提供缓存。
 *
 * DM 将文件系统抽象成页面，每次对文件系统的读写都是以页面为单位的。
 * 同样，从文件系统读进来的数据也是以页面为单位进行缓存的。
 */
public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    /**
     * 从已有文件创建 DataManager 和从空文件创建 DataManager 的流程稍有不同，
     * 除了 PageCache 和 Logger 的创建方式有所不同以外，从空文件创建首先需要对第一页进行初始化，
     * 而从已有文件创建，则是需要对第一页进行校验，来判断是否需要执行恢复流程。并重新对第一页生成随机字节。
     * @param path
     * @param mem
     * @param tm
     * @return
     */
    static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
