package com.basho.riak.pbc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class SimpleRiakResource implements RiakResource {

    private InetSocketAddress addr;
    private RiakConnection connection;

    public SimpleRiakResource(InetAddress addr, int port) {
        this.addr = new InetSocketAddress(addr, port);
    }

    public void initialize() {
        try {
            this.connection = new RiakConnection(this.addr);
            this.connection.open();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public RiakConnection allocate() {
        return this.connection;
    }

    public void release(RiakConnection connection) {}

    public void dispose() {
        this.connection.close();
    }
}
