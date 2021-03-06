package com.rethinkdb.impl;

import com.rethinkdb.RqlCursor;
import java.util.Iterator;
import java.util.NoSuchElementException;
import com.rethinkdb.RqlDriverException;
import com.rethinkdb.Ql2.Response;

public class RqlCursorImpl implements RqlCursor {

    private final RqlConnectionImpl _connection;
    private Response _response;
    private int _index;

    public RqlCursorImpl(RqlConnectionImpl conn, Response rsp) {
        _connection = conn;
        _response = rsp;
        _index = 0;
    }

    @Override
    public Iterator<RqlObject> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return _response.getType() == Response.ResponseType.SUCCESS_PARTIAL || _index < _response.getResponseCount();
    }

    @Override
    public RqlObject next() {
        if (_index < _response.getResponseCount()) {
            return new RqlObject(_response.getResponse(_index++));
        } 
        else if (_response.getType() == Response.ResponseType.SUCCESS_PARTIAL) {
            try {
                _response = _connection.get_more(_response.getToken());
                _index = 0;
                return next();
            } 
            catch (RqlDriverException e) {
                throw new NoSuchElementException(e.getMessage());
            }
        }
        
        throw new NoSuchElementException("The RqlCursor has no more elements");
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removing rows from a RqlCursor is not currently supported");
    }

    @Override
    public String toString() {
        return _response.toString();
    }
}
