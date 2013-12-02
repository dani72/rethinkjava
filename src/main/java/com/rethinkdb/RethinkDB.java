package com.rethinkdb;

import com.rethinkdb.impl.RqlConnectionImpl;

/**
 *
 * @author dani
 */
public final class RethinkDB {
    private RethinkDB() {
    }
    
    public static RqlConnection connect( String host, int port) {
        return new RqlConnectionImpl( host, port);
    }
}
