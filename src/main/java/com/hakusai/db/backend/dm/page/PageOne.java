package com.hakusai.db.backend.dm.page;

import java.util.Arrays;

import com.hakusai.db.backend.dm.pageCache.PageCache;
import com.hakusai.db.backend.utils.RandomUtil;

/**
 数据库文件的第一页，通常用作一些特殊用途，比如存储一些元数据，用来启动检查什么的。
 MYDB 的第一页，只是用来做启动检查。具体的原理是，
 在每次数据库启动时，会生成一串随机字节，存储在 100 ~ 107 字节。
 在数据库正常关闭时，会将这串字节，拷贝到第一页的 108 ~ 115 字节。

 这样数据库在每次启动时，就会检查第一页两处的字节是否相同，以此来判断上一次是否正常关闭。
 如果是异常关闭，就需要执行数据的恢复流程。
 */
public class PageOne {
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    /**
     * 启动时设置初始字节
     * @param pg
     */
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    /**
     * 关闭时拷贝字节
     * @param pg
     */
    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }

    /**
     * 校验字节
     * @param pg
     * @return
     */
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC+LEN_VC), Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC));
        //return Arrays.compare(raw, OF_VC, OF_VC+LEN_VC, raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC) == 0;
    }
}
