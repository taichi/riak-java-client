package com.basho.riak.pbc;

public interface RiakResource {

    void initialize();

    RiakConnection allocate();

    void release(RiakConnection connection);

    void dispose();
}
