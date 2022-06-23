package com.hakusai.db.backend.tm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.hakusai.db.backend.utils.Panic;
import com.hakusai.db.backend.utils.Parser;
import com.hakusai.db.common.Error;


/**
 * Copyright (c) 2022, Bongmi
 * All rights reserved
 * Author: yinpeng@bongmi.com
 */

public class TransactionManagerImpl implements TransactionManager {

  /**
   * XID 文件给每个事务分配了一个字节的空间，用来保存其状态。
   * 同时，在 XID 文件的头部，还保存了一个 8 字节的数字，记录了这个 XID 文件管理的事务的个数。
   * 于是，事务 xid 在文件中的状态就存储在 (xid-1)+8 字节处，xid-1 是因为 xid 0（Super XID） 的状态不需要记录。
   */
  // XID文件头长度
  static final int LEN_XID_HEADER_LENGTH = 8;
  // 每个事务的占用长度
  private static final int XID_FIELD_SIZE = 1;

  // 事务的三种状态
  private static final byte FIELD_TRAN_ACTIVE = 0;
  private static final byte FIELD_TRAN_COMMITTED = 1;
  private static final byte FIELD_TRAN_ABORTED = 2;

  /**
   * 在 MYDB 中，每一个事务都有一个 XID，这个 ID 唯一标识了这个事务。事务的 XID 从 1 开始标号，并自增，不可重复。
   * 并特殊规定 XID 0 是一个超级事务（Super Transaction）。
   * 当一些操作想在没有申请事务的情况下进行，那么可以将操作的 XID 设置为 0。XID 为 0 的事务的状态永远是 committed。
   */
  public static final long SUPER_XID = 0;

  static final String XID_SUFFIX = ".xid";

  private RandomAccessFile file;
  private FileChannel fc;
  private long xidCounter;
  private Lock counterLock;

  TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
    this.file = raf;
    this.fc = fc;
    counterLock = new ReentrantLock();
    checkXIDCounter();
  }

  /**
   * 在构造函数创建了一个 TransactionManager 之后，首先要对 XID 文件进行校验，以保证这是一个合法的 XID 文件。
   * 校验的方式也很简单，通过文件头的 8 字节数字反推文件的理论长度，与文件的实际长度做对比。
   * 如果不同则认为 XID 文件不合法。
   * 对于校验没有通过的，会直接通过 panic 方法，强制停机。在一些基础模块中出现错误都会如此处理，无法恢复的错误只能直接停机。
   */
  private void checkXIDCounter() {
    long fileLen = 0;
    try {
      fileLen = file.length();
    } catch (IOException e1) {
      Panic.panic(Error.BadXIDFileException);
    }
    if (fileLen < LEN_XID_HEADER_LENGTH) {
      Panic.panic(Error.BadXIDFileException);
    }

    ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
    try {
      fc.position(0);
      fc.read(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }
    this.xidCounter = Parser.parseLong(buf.array());
    long end = getXidPosition(this.xidCounter + 1);
    if (end != fileLen) {
      Panic.panic(Error.BadXIDFileException);
    }
  }

  // 根据事务xid取得其在xid文件中对应的位置
  private long getXidPosition(long xid) {
    return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
  }

  // 更新xid事务的状态为status
  private void updateXID(long xid, byte status) {
    long offset = getXidPosition(xid);
    byte[] tmp = new byte[XID_FIELD_SIZE];
    tmp[0] = status;
    ByteBuffer buf = ByteBuffer.wrap(tmp);
    try {
      fc.position(offset);
      fc.write(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }
    try {
      /*×
      这里的所有文件操作，在执行后都需要立刻刷入文件中，防止在崩溃后文件丢失数据，f
      ileChannel 的 force() 方法，强制同步缓存内容到文件中，类似于 BIO 中的 flush() 方法。
      force 方法的参数是一个布尔，表示是否同步文件的元数据
       */
      fc.force(false);
    } catch (IOException e) {
      Panic.panic(e);
    }
  }

  // 将XID加一，并更新XID Header
  private void incrXIDCounter() {
    xidCounter++;
    ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
    try {
      fc.position(0);
      fc.write(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }
    try {
      fc.force(false);
    } catch (IOException e) {
      Panic.panic(e);
    }
  }

  // 开始一个事务，并返回XID
  public long begin() {
    counterLock.lock();
    try {
      long xid = xidCounter + 1;
      updateXID(xid, FIELD_TRAN_ACTIVE);
      incrXIDCounter();
      return xid;
    } finally {
      counterLock.unlock();
    }
  }

  // 提交XID事务
  public void commit(long xid) {
    updateXID(xid, FIELD_TRAN_COMMITTED);
  }

  // 回滚XID事务
  public void abort(long xid) {
    updateXID(xid, FIELD_TRAN_ABORTED);
  }

  // 检测XID事务是否处于status状态
  private boolean checkXID(long xid, byte status) {
    long offset = getXidPosition(xid);
    ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
    try {
      fc.position(offset);
      fc.read(buf);
    } catch (IOException e) {
      Panic.panic(e);
    }
    return buf.array()[0] == status;
  }

  public boolean isActive(long xid) {
    if (xid == SUPER_XID) {
      return false;
    }
    return checkXID(xid, FIELD_TRAN_ACTIVE);
  }

  public boolean isCommitted(long xid) {
    if (xid == SUPER_XID) {
      return true;
    }
    return checkXID(xid, FIELD_TRAN_COMMITTED);
  }

  public boolean isAborted(long xid) {
    if (xid == SUPER_XID) {
      return false;
    }
    return checkXID(xid, FIELD_TRAN_ABORTED);
  }

  public void close() {
    try {
      fc.close();
      file.close();
    } catch (IOException e) {
      Panic.panic(e);
    }
  }

}
