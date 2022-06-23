package com.hakusai.db.client;

import com.hakusai.db.transport.Package;
import com.hakusai.db.transport.Packager;

/**
 * 客户端连接服务器的过程，也是背板。客户端有一个简单的 Shell，
 * 实际上只是读入用户的输入，并调用 Client.execute()。
 */
public class Client {
    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPkg = rt.roundTrip(pkg);
        if(resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }

}
