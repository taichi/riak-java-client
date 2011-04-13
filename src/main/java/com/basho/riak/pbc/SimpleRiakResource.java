package com.basho.riak.pbc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class SimpleRiakResource implements RiakResource {

    private InetSocketAddress addr;

    private ThreadLocal<RiakConnection> slot = new ThreadLocal<RiakConnection>();

    public SimpleRiakResource(InetAddress addr, int port) {
        this.addr = new InetSocketAddress(addr, port);
    }

    public void initialize() {}

    public RiakConnection allocate() {
        try {
            RiakConnection connection = this.slot.get();
            if ((connection == null) || connection.isClosed()) {
                connection = new RiakConnection(this.addr);
                connection.open();
            }
            this.slot.set(null);
            return connection;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void release(RiakConnection connection) {
        RiakConnection rc = this.slot.get();
        if (rc == null) {
            this.slot.set(connection);
        } else {
            rc.close();
        }
    }

    public void dispose() {
        RiakConnection connection = this.slot.get();
        if (connection != null) {
            connection.close();
        }
    }
}
