package com.rethinkdb.impl;

import com.rethinkdb.RqlConnection;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import com.rethinkdb.RqlDriverException;
import com.rethinkdb.Ql2.Query;
import com.rethinkdb.Ql2.Response;

public class RqlConnectionImpl implements RqlConnection {

    private final RqlSocketConnection _sc;

    // A counter for the request tokens. Request tokens must be unique per connection
    private final AtomicLong counter = new AtomicLong(0);

    public RqlConnectionImpl(String host, int port) {
        try {
            _sc = new RqlSocketConnection( host, port);
        }
        catch( IOException e) {
            throw new RqlDriverException( "Could not connect to server <" + host + "> on port <" + port + ">.", e);
        }
    }

    @Override
    public void close() {
        try {
            _sc.close();
        }
        catch( IOException e) {
            throw new RqlDriverException( "Could not close connection.", e);
        }
    }

    @Override
    public RqlCursorImpl execute(RqlQuery query) {
        Query.Builder q = com.rethinkdb.Ql2.Query.newBuilder();

        q.setType(Query.QueryType.START);
        q.setToken(nextToken());
        q.setQuery(query.build());

        send( q);

        Response rsp = receive();

        // For this version we only support success :-(
        switch (rsp.getType()) {
            case SUCCESS_ATOM:
            case SUCCESS_SEQUENCE:
            case SUCCESS_PARTIAL:
                return new RqlCursorImpl(this, rsp);
            case CLIENT_ERROR:
            case COMPILE_ERROR:
            case RUNTIME_ERROR:
            default:
                throw new RqlDriverException(rsp.toString());
        }
    }

    public Response get_more(long token) {
        // Send the [CONTINUE] query 
        Query.Builder q = com.rethinkdb.Ql2.Query.newBuilder()
                                                    .setType(Query.QueryType.CONTINUE)
                                                    .setToken(token);

        send( q);

        return receive();
    }

    /* Utility functions to make a pretty API */
    @Override
    public RqlQuery.Table table(Object... args) {
        return new RqlQuery.Table( args);
    }

    @Override
    public RqlTopLevelQuery.DB db(Object... args) {
        return new RqlTopLevelQuery.DB( args);
    }

    @Override
    public RqlTopLevelQuery.DbCreate db_create(Object... args) {
        return new RqlTopLevelQuery.DbCreate( args);
    }

    @Override
    public RqlTopLevelQuery.DbDrop db_drop(Object... args) {
        return new RqlTopLevelQuery.DbDrop( args);
    }

    @Override
    public RqlTopLevelQuery.DbList db_list(Object... args) {
        return new RqlTopLevelQuery.DbList( args);
    }

    private long nextToken() {
        return counter.incrementAndGet();
    }

    private void send( Query.Builder qb) {
        try {
            _sc.send( qb.build().toByteArray());
        }
        catch( IOException e) {
            throw new RqlDriverException( "Could not send request.", e);
        }
    }

    private Response receive() {
        try {
            return com.rethinkdb.Ql2.Response.parseFrom( _sc.receive().array());
        }
        catch( IOException e) {
            throw new RqlDriverException( "Could not receive response.", e);
        }
    }
}
