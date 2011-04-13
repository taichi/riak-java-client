package com.basho.riak.pbc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class SimpleRiakResource implements RiakResource {

    private InetSocketAddress addr;

    public SimpleRiakResource(String hostname) {
        this(hostname, RiakConnection.DEFAULT_RIAK_PB_PORT);
    }

    public SimpleRiakResource(String hostname, int port) {
        this.addr = new InetSocketAddress(hostname, port);
    }

    public SimpleRiakResource(InetAddress addr, int port) {
        this.addr = new InetSocketAddress(addr, port);
    }

    public RiakConnection allocate() {
        try {
            RiakConnection connection = new RiakConnection(this.addr);
            connection.open();
            return connection;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void release(RiakConnection connection) {
        connection.close();
    }
}
