/**
 * This file is part of riak-java-pb-client
 * 
 * Copyright (c) 2010 by Trifork
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

package com.basho.riak.pbc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import com.basho.riak.pbc.RPB.RpbErrorResp;
import com.google.protobuf.MessageLite;

class RiakConnection {

    static final int DEFAULT_RIAK_PB_PORT = 8087;
    static final int SOCKET_BUFFER_SIZE = 1024 * 200;
    static final int CONNECTION_TIMEOUT = 1000;

    private InetSocketAddress addr;
    private Socket sock;
    private DataOutputStream dout;
    private DataInputStream din;

    private boolean clientIdSet = false;

    public RiakConnection(InetAddress addr, int port) throws IOException {
        this(new InetSocketAddress(addr, port));
    }

    public RiakConnection(InetSocketAddress addr) {
        this.sock = new Socket();
        this.addr = addr;
    }

    void open() throws IOException {
        this.sock.setSendBufferSize(SOCKET_BUFFER_SIZE);
        // this.sock.setSoTimeout(CONNECTION_TIMEOUT);

        this.sock.connect(this.addr);

        this.dout = new DataOutputStream(new BufferedOutputStream(this.sock.getOutputStream(), SOCKET_BUFFER_SIZE));
        this.din = new DataInputStream(new BufferedInputStream(this.sock.getInputStream(), SOCKET_BUFFER_SIZE));
    }

    boolean isClientIdSet() {
        return this.clientIdSet;
    }

    // /////////////////////

    void send(int code, MessageLite req) throws IOException {
        int len = req.getSerializedSize();
        this.dout.writeInt(len + 1);
        this.dout.write(code);
        req.writeTo(this.dout);
        this.dout.flush();
    }

    void send(int code) throws IOException {
        this.dout.writeInt(1);
        this.dout.write(code);
        this.dout.flush();
    }

    byte[] receive(int code) throws IOException {
        int len = this.din.readInt();
        int get_code = this.din.read();

        if (code == RiakClient.MSG_ErrorResp) {
            RpbErrorResp err = com.basho.riak.pbc.RPB.RpbErrorResp.parseFrom(this.din);
            throw new RiakError(err);
        }

        byte[] data = null;
        if (len > 1) {
            data = new byte[len - 1];
            this.din.readFully(data);
        }

        if (code != get_code) {
            throw new IOException("bad message code");
        }

        return data;
    }

    void receive_code(int code) throws IOException, RiakError {
        int len = this.din.readInt();
        int get_code = this.din.read();
        if (code == RiakClient.MSG_ErrorResp) {
            RpbErrorResp err = com.basho.riak.pbc.RPB.RpbErrorResp.parseFrom(this.din);
            throw new RiakError(err);
        }
        if ((len != 1) || (code != get_code)) {
            throw new IOException("bad message code");
        }
        if (code == RiakClient.MSG_SetClientIdResp) {
            this.clientIdSet = true;
        }
    }

    void close() {
        if (isClosed()) {
            return;
        }

        try {
            this.sock.close();
            this.din = null;
            this.dout = null;
            this.sock = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public DataOutputStream getOutputStream() {
        return this.dout;
    }

    public boolean isClosed() {
        return (this.sock == null) || this.sock.isClosed();
    }

}
