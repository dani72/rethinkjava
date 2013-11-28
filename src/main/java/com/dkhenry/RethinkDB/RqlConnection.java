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
public interface RqlConnection extends AutoCloseable {

    @Override
    void close() throws RqlDriverException;

    RqlTopLevelQuery.DB db(Object... args);

    RqlTopLevelQuery.DbCreate db_create(Object... args);

    RqlTopLevelQuery.DbDrop db_drop(Object... args);

    RqlTopLevelQuery.DbList db_list(Object... args);

    RqlQuery.Table table(Object... args);
    
    RqlCursor run(RqlQuery query) throws RqlDriverException;
}
