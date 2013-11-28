/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.dkhenry.RethinkDB;

import com.dkhenry.RethinkDB.errors.RqlDriverException;

/**
 *
 * @author dani
 */
public final class RethinkDB {
    private RethinkDB() {
    }
    
    public static RqlConnection connect( String host, int port) throws RqlDriverException {
        return RqlConnectionImpl.connect( host, port);
    }
}
