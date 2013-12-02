package com.rethinkdb.impl;

import java.util.List;
import java.util.Map;

public class RqlObject {

    private final com.rethinkdb.Ql2.Datum _underlying;

    public RqlObject(com.rethinkdb.Ql2.Datum d) {
        _underlying = d;
    }

    public boolean getBoolean() {
        return as();
    }

    public double getNumber() {
        return as();
    }

    public String getString() {
        return as();
    }

    public List<Object> getList() {
        return as();
    }

    public Map<String, Object> getMap() {
        return as();
    }

    @SuppressWarnings("unchecked")
    public <T> T as() {
        return (T) Datum.deconstruct(_underlying);
    }

    // The next few function will assume this is of type "RObject" 
    @SuppressWarnings("unchecked")
    public <T> T getAs(String key) {
        Map<String, Object> m = as();
        return (T) m.get(key);
    }

    public <T> T getAsOrElse(String key, T orElse) {
        T rval = getAs(key);
        if (null == rval) {
            return orElse;
        }
        return rval;
    }

    @Override
    public String toString() {
        return _underlying.toString();
    }
}
