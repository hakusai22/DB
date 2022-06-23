package com.hakusai.db.backend.vm;

import com.hakusai.db.backend.tm.TransactionManager;

public class Visibility {

    /**
     * 版本跳跃问题，考虑如下的情况，假设 X 最初只有 x0 版本，T1 和 T2 都是可重复读的隔离级别：
     *
     * T1 begin
     * T2 begin
     * R1(X) // T1读取x0
     * R2(X) // T2读取x0
     * U1(X) // T1将X更新到x1
     * T1 commit
     * U2(X) // T2将X更新到x2
     * T2 commit
     *
     * 这种情况实际运行起来是没问题的，但是逻辑上不太正确。
     * T1 将 X 从 x0 更新为了 x1，这是没错的。
     * 但是 T2 则是将 X 从 x0 更新成了 x2，跳过了 x1 版本。
     *
     * 读提交是允许版本跳跃的，而可重复读则是不允许版本跳跃的。
     * 解决版本跳跃的思路也很简单：如果 Ti 需要修改 X，而 X 已经被 Ti 不可见的事务 Tj 修改了，那么要求 Ti 回滚。
     *
     * 于是版本跳跃的检查也就很简单了，取出要修改的数据 X 的最新提交版本，并检查该最新版本的创建者对当前事务是否可见
     * @param tm
     * @param t
     * @param e
     * @return
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 上面提到，如果一个记录的最新版本被加锁，当另一个事务想要修改或读取这条记录时，
     * MYDB 就会返回一个较旧的版本的数据。这时就可以认为，最新的被加锁的版本，对于另一个事务来说，是不可见的。
     * 于是版本可见性的概念就诞生了。
     *
     * 版本的可见性与事务的隔离度是相关的。MYDB 支持的最低的事务隔离程度，是“读提交”（Read Committed），
     * 即事务在读取数据时, 只能读取已经提交事务产生的数据。保证最低的读提交的好处，
     *
     * MYDB 实现读提交，为每个版本维护了两个变量，就是上面提到的 XMIN 和 XMAX：
     * XMIN：创建该版本的事务编号
     * XMAX：删除该版本的事务编号
     * XMIN 应当在版本创建时填写，而 XMAX 则在版本被删除，或者有新版本出现时填写。
     *
     * XMAX 这个变量，也就解释了为什么 DM 层不提供删除操作，
     * 当想删除一个版本时，只需要设置其 XMAX，这样，这个版本对每一个 XMAX 之后的事务都是不可见的，也就等价于删除了。
     *
     * 若条件为 true，则版本对 Ti 可见。那么获取 Ti 适合的版本，只需要从最新版本开始，
     * 依次向前检查可见性，如果为 true，就可以直接返回。
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;

        if(tm.isCommitted(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 不可重复度，会导致一个事务在执行期间对同一个数据项的读取得到不同结果。如下面的结果，加入 X 初始值为 0：
     *
     * T1 begin
     * R1(X) // T1 读得 0
     * T2 begin
     * U2(X) // 将 X 修改为 1
     * T2 commit
     * R1(X) // T1 读的 1
     * 可以看到，T1 两次读 X，读到的结果不一样。如果想要避免这个情况，就需要引入更严格的隔离级别，即可重复读（repeatable read）。
     * T1 在第二次读取的时候，读到了已经提交的 T2 修改的值，导致了这个问题。于是我们可以规定：
     *
     * :::primary
     * 事务只能读取它开始时, 就已经结束的那些事务产生的数据版本
     *
     * 这条规定，增加于，事务需要忽略：
     *  1. 在本事务后开始的事务的数据;
     *  2. 本事务开始时还是 active 状态的事务的数据
     *
     * 构造方法中的 active，保存着当前所有 active 的事务。于是，可重复读的隔离级别下，一个版本是否对事务可见的判断如下：
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;

        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
