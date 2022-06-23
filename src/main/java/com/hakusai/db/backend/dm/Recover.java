package com.hakusai.db.backend.dm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.primitives.Bytes;

import com.hakusai.db.backend.common.SubArray;
import com.hakusai.db.backend.dm.dataItem.DataItem;
import com.hakusai.db.backend.dm.logger.Logger;
import com.hakusai.db.backend.dm.page.Page;
import com.hakusai.db.backend.dm.page.PageX;
import com.hakusai.db.backend.dm.pageCache.PageCache;
import com.hakusai.db.backend.tm.TransactionManager;
import com.hakusai.db.backend.utils.Panic;
import com.hakusai.db.backend.utils.Parser;

/**
 * DM 为上层模块，提供了两种操作，分别是插入新数据（I）和更新现有数据（U）。
 * 在进行 I 和 U 操作之前，必须先进行对应的日志操作，在保证日志写入磁盘后，才进行数据操作。
 *
 * 日志在数据操作之前，保证到达了磁盘，那么即使该数据操作最后没有来得及同步到磁盘，
 * 数据库就发生了崩溃，后续也可以通过磁盘上的日志恢复该数据。
 *
 * 对于两种数据操作，DM 记录的日志如下：
 * (Ti, I, A, x)，表示事务 Ti 在 A 位置插入了一条数据 x
 * (Ti, U, A, oldx, newx)，表示事务 Ti 将 A 位置的数据，从 oldx 更新成 newx
 */

/**
 * 我们首先不考虑并发的情况，那么在某一时刻，只可能有一个事务在操作数据库。日志会看起来像下面那样：
 * (Ti, x, x), ..., (Ti, x, x), (Tj, x, x), ..., (Tj, x, x), (Tk, x, x), ..., (Tk, x, x)
 *
 * 单线程
 * 由于单线程，Ti、Tj 和 Tk 的日志永远不会相交。这种情况下利用日志恢复很简单，假设日志中最后一个事务是 Ti：
 *
 * 对 Ti 之前所有的事务的日志，进行重做（redo）
 * 接着检查 Ti 的状态（XID 文件），如果 Ti 的状态是已完成（包括 committed 和 aborted），就将 Ti 重做，否则进行撤销（undo）
 * 接着，是如何对事务 T 进行 redo：
 *
 * 正序扫描事务 T 的所有日志
 * 如果日志是插入操作 (Ti, I, A, x)，就将 x 重新插入 A 位置
 * 如果日志是更新操作 (Ti, U, A, oldx, newx)，就将 A 位置的值设置为 newx
 * undo 也很好理解：
 *
 * 倒序扫描事务 T 的所有日志
 * 如果日志是插入操作 (Ti, I, A, x)，就将 A 位置的数据删除
 * 如果日志是更新操作 (Ti, U, A, oldx, newx)，就将 A 位置的值设置为 oldx
 * 注意，MYDB 中其实没有真正的删除操作，对于插入操作的 undo，只是将其中的标志位设置为 invalid。对于删除的探讨将在 VM 一节中进行。
 */

/**
 * 第一种：
 *
 * T1 begin
 * T2 begin
 * T2 U(x)
 * T1 R(x)
 * ...
 * T1 commit
 * MYDB break down

 * 在系统崩溃时，T2 仍然是活跃状态。那么当数据库重新启动，执行恢复例程时，会撤销 T2，它对数据库的影响会被消除。
 * 但是由于 T1 读取了 T2 更新的值，既然 T2 被撤销，那么 T1 也应当被撤销。
 * 这种情况，就是级联回滚。但是，T1 已经 commit 了，所有 commit 的事务的影响，应当被持久化。这里就造成了矛盾。
 * 所以这里需要保证：
 * 规定1：正在进行的事务，不会读取其他任何未提交的事务产生的数据。
 *
 *
 * 第二种情况，假设 x 的初值是 0
 *
 * T1 begin
 * T2 begin
 * T1 set x = x+1 // 产生的日志为(T1, U, A, 0, 1)
 * T2 set x = x+1 // 产生的日志为(T1, U, A, 1, 2)
 * T2 commit
 * MYDB break down
 *
 * 在系统崩溃时，T1 仍然是活跃状态。那么当数据库重新启动，执行恢复例程时，会对 T1 进行撤销，对 T2 进行重做，
 * 但是，无论撤销和重做的先后顺序如何，x 最后的结果，要么是 0，要么是 2，这都是错误的。
 *
 * MYDB 采用的是限制数据库操作，需要保证：
 *
 * 规定2：正在进行的事务，不会修改其他任何未提交的事务修改或产生的数据。
 *
 * 有了这两条规定，并发情况下日志的恢复也就很简单了：
 *
 * 重做所有崩溃时已完成（committed 或 aborted）的事务
 * 撤销所有崩溃时未完成（active）的事务
 * 在恢复后，数据库就会恢复到所有已完成事务结束，所有未完成事务尚未开始的状态。
 */
public class Recover {

    //insert
    private static final byte LOG_TYPE_INSERT = 0;
    //update
    private static final byte LOG_TYPE_UPDATE = 1;
    //redolog
    private static final int REDO = 0;
    //undolog
    private static final int UNDO = 1;

    /**
     * updateLog:
     * [LogType] [XID] [UID] [OldRaw] [NewRaw]
     *
     */
    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    /**
     * insertLog:
     * [LogType] [XID] [Pgno] [Offset] [Raw]
     */
    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    /**
     * 和原理中描述的类似，recover 例程主要也是两步：重做所有已完成事务，撤销所有未完成事务
     * @param tm
     * @param lg
     * @param pc
     */
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        lg.rewind();
        int maxPgno = 0;
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            int pgno;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if(maxPgno == 0) {
            maxPgno = 1;
        }
        pc.truncateByBgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    /**
     *
     * @param tm
     * @param lg
     * @param pc
     */
    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有active log进行倒序undo
        for(Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE+1;
    private static final int OF_UPDATE_UID = OF_XID+8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8;

    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }

    /**
     * updateLog 和 insertLog 的重做和撤销处理，分别合并成一个方法来实现：
     * @param pc
     * @param log
     * @param flag
     */
    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        if(flag == REDO) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID+8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2;

    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(li.pgno);
        } catch(Exception e) {
            Panic.panic(e);
        }
        try {
            /**
             * 注意，doInsertLog() 方法中的删除，使用的是 DataItem.setDataItemRawInvalid(li.raw);
             * 大致的作用，就是将该条 DataItem 的有效位设置为无效，来进行逻辑删除。
             */
            if(flag == UNDO) {
                DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }
}
