package com.hakusai.db.backend.tbm;

import com.hakusai.db.backend.dm.DataManager;
import com.hakusai.db.backend.parser.statement.Select;
import com.hakusai.db.backend.vm.VersionManager;
import com.hakusai.db.backend.parser.statement.Begin;
import com.hakusai.db.backend.parser.statement.Create;
import com.hakusai.db.backend.parser.statement.Delete;
import com.hakusai.db.backend.parser.statement.Insert;
import com.hakusai.db.backend.parser.statement.Update;
import com.hakusai.db.backend.utils.Parser;

/**
 * 由于 TableManager 已经是直接被最外层 Server 调用（MYDB 是 C/S 结构），
 * 这些方法直接返回执行的结果，例如错误信息或者结果信息的字节数组（可读）。
 */
public interface TableManager {
    BeginRes begin(Begin begin);
    byte[] commit(long xid) throws Exception;
    byte[] abort(long xid);

    byte[] show(long xid);
    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;
    byte[] read(long xid, Select select) throws Exception;
    byte[] update(long xid, Update update) throws Exception;
    byte[] delete(long xid, Delete delete) throws Exception;

    /**
     * 唯一值得注意的一个小点是，在创建新表时，采用的时头插法，所以每次创建表都需要更新 Booter 文件。
     * @param path
     * @param vm
     * @param dm
     * @return
     */
    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
