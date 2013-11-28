package com.dkhenry.RethinkDB;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

import com.dkhenry.RethinkDB.errors.RqlDriverException;
import com.rethinkdb.Ql2.Query;
import com.rethinkdb.Ql2.Response;

public class RqlConnectionImpl implements RqlConnection {

    private SocketChannel _sc;
    private String _hostname;
    private int _port;
    private boolean _connected;

    //! A global counter for the request tokens; 
    private static final AtomicLong counter = new AtomicLong(0);

    public RqlConnectionImpl() {
        _connected = false;
    }

    public String get_hostname() {
        return _hostname;
    }

    public void set_hostname(String hostname) throws RqlDriverException {
        String ohostname = _hostname;
        _hostname = hostname;
        if (_connected && (hostname == null ? ohostname != null : !hostname.equals(ohostname))) {
            reconnect();
        }
    }

    public int get_port() {
        return _port;
    }

    public void set_port(int port) throws RqlDriverException {
        int oport = _port;
        _port = port;
        if (_connected && oport != port) {
            reconnect();
        }
    }

    @Override
    public void close() throws RqlDriverException {
        if (_connected) {
            try {
                _sc.close();
            } catch (IOException e) {
                throw new RqlDriverException("Could not close connection.", e);
            }
            _connected = false;
        }
    }

    @Override
    public RqlCursor run(RqlQuery query) throws RqlDriverException {
        Query.Builder q = com.rethinkdb.Ql2.Query.newBuilder();
        q.setType(Query.QueryType.START);
        q.setToken(nextToken());
        q.setQuery(query.build());
        try {
            send_raw(q.build().toByteArray());
        } catch (IOException e) {
            throw new RqlDriverException( "Could not execute query.", e);
        }
        Response rsp = get();

        // For this version we only support success :-(
        switch (rsp.getType()) {
            case SUCCESS_ATOM:
            case SUCCESS_SEQUENCE:
            case SUCCESS_PARTIAL:
                return new RqlCursor(this, rsp);
            case CLIENT_ERROR:
            case COMPILE_ERROR:
            case RUNTIME_ERROR:
            default:
                throw new RqlDriverException(rsp.toString());
        }
    }

    public Response get() throws RqlDriverException {
        try {
            return recv_raw();
        } catch (IOException e) {
            throw new RqlDriverException("Could not get response.", e);
        }
    }

    public Response get_more(long token) throws RqlDriverException {
        // Send the [CONTINUE] query 
        Query.Builder q = com.rethinkdb.Ql2.Query.newBuilder()
                .setType(Query.QueryType.CONTINUE)
                .setToken(token);
        try {
            send_raw(q.build().toByteArray());
            return recv_raw();
        } catch (IOException e) {
            throw new RqlDriverException( "Could not get more data.", e);
        }
    }

    /* Utility functions to make a pretty API */
    @Override
    public RqlQuery.Table table(Object... args) {
        RqlQuery.Table rvalue = new RqlQuery.Table();
        rvalue.construct(args);
        return rvalue;
    }

    @Override
    public RqlTopLevelQuery.DB db(Object... args) {
        RqlTopLevelQuery.DB rvalue = new RqlTopLevelQuery.DB();
        rvalue.construct(args);
        return rvalue;
    }

    @Override
    public RqlTopLevelQuery.DbCreate db_create(Object... args) {
        RqlTopLevelQuery.DbCreate rvalue = new RqlTopLevelQuery.DbCreate();
        rvalue.construct(args);
        return rvalue;
    }

    @Override
    public RqlTopLevelQuery.DbDrop db_drop(Object... args) {
        RqlTopLevelQuery.DbDrop rvalue = new RqlTopLevelQuery.DbDrop();
        rvalue.construct(args);
        return rvalue;
    }

    @Override
    public RqlTopLevelQuery.DbList db_list(Object... args) {
        RqlTopLevelQuery.DbList rvalue = new RqlTopLevelQuery.DbList();
        rvalue.construct(args);
        return rvalue;
    }

    /* Private methods */
    private void reconnect() throws RqlDriverException {
        try {
            if (_connected) {
                _sc.close();
            }
            _sc = SocketChannel.open();
            _sc.configureBlocking(true);
            _sc.connect(new InetSocketAddress(_hostname, _port));

            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putInt(com.rethinkdb.Ql2.VersionDummy.Version.V0_1_VALUE);

            buffer.flip();
            while (buffer.hasRemaining()) {
                _sc.write(buffer);
            }

            _connected = true;
        } catch (IOException e) {
            throw new RqlDriverException("Could not reconnect.", e);
        }

    }

    public long nextToken() {
        return counter.incrementAndGet();
    }

    public void send_raw(byte[] data) throws IOException {
        rethink_send(_sc, data);
    }

    public Response recv_raw() throws IOException {
        return rethink_recv(_sc);
    }

    public static RqlConnectionImpl connect(String hostname, int port) throws RqlDriverException {
        RqlConnectionImpl r = new RqlConnectionImpl();
        r.set_hostname(hostname);
        r.set_port(port);
        r.reconnect();
        return r;
    }

    public static void rethink_send(SocketChannel sc, byte[] data) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4 + data.length);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.flip();

        while (buffer.hasRemaining()) {
            sc.write(buffer);
        }
    }

    public static Response rethink_recv(SocketChannel sc) throws IOException {
        ByteBuffer datalen = ByteBuffer.allocate(4);
        datalen.order(ByteOrder.LITTLE_ENDIAN);
        int bytesRead = sc.read(datalen);
        if (bytesRead != 4) {
            throw new IOException("Incorrect amount of data read " + (new Integer(bytesRead)).toString() + " (expected 4) ");
        }
        datalen.flip();
        int len = datalen.getInt();

        ByteBuffer buf = ByteBuffer.allocate(len);
        bytesRead = 0;
        while (bytesRead != len) {
            bytesRead += sc.read(buf);
        }
        buf.flip();
        com.rethinkdb.Ql2.Response r = com.rethinkdb.Ql2.Response.parseFrom(buf.array());
        return r;
    }
}
