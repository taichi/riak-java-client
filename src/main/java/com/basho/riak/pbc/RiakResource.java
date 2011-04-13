package com.basho.riak.pbc;

public interface RiakResource {

    RiakConnection allocate();

    void release(RiakConnection connection);
}
