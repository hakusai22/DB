package com.hakusai.db.backend.tm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.hakusai.db.backend.utils.Panic;
import com.hakusai.db.common.Error;

/**
 * Copyright (c) 2022, Bongmi
 * All rights reserved
 * Author: yinpeng@bongmi.com
 */

public interface TransactionManager {
  long begin();                       // 开启一个新事务

  void commit(long xid);              // 提交一个事务

  void abort(long xid);               // 取消一个事务

  boolean isActive(long xid);         // 查询一个事务的状态是否是正在进行的状态

  boolean isCommitted(long xid);      // 查询一个事务的状态是否是已提交

  boolean isAborted(long xid);        // 查询一个事务的状态是否是已取消

  void close();                       // 关闭TM


  /**
   * 另外就是两个静态方法：create() 和 open()，分别表示创建一个 xid 文件并创建 TM 和从一个已有的 xid 文件来创建 TM。
   * 从零创建 XID 文件时需要写一个空的 XID 文件头，即设置 xidCounter 为 0，否则后续在校验时会不合法：
   *
   * @param path
   * @return
   */
  static TransactionManagerImpl create(String path) {
    File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
    try {
      if (!f.createNewFile()) {
        Panic.panic(Error.FileExistsException);
      }
    } catch (Exception e) {
      Panic.panic(e);
    }
    if (!f.canRead() || !f.canWrite()) {
      Panic.panic(Error.FileCannotRWException);
    }

    FileChannel fc = null;
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(f, "rw");
      fc = raf.getChannel();
    } catch (FileNotFoundException e) {
      Panic.panic(e);
    }

    // 写空XID文件头
    ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
    try {
      fc.position(0);
      fc.write(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }

    return new TransactionManagerImpl(raf, fc);
  }

  static TransactionManagerImpl open(String path) {
    File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
    if (!f.exists()) {
      Panic.panic(Error.FileNotExistsException);
    }
    if (!f.canRead() || !f.canWrite()) {
      Panic.panic(Error.FileCannotRWException);
    }

    FileChannel fc = null;
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(f, "rw");
      fc = raf.getChannel();
    } catch (FileNotFoundException e) {
      Panic.panic(e);
    }

    return new TransactionManagerImpl(raf, fc);
  }
}
