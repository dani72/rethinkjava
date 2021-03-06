package com.rethinkdb;

import com.rethinkdb.impl.RqlQuery;
import com.rethinkdb.impl.RqlTopLevelQuery;

/**
 *
 * @author dani
 */
public interface RqlConnection extends AutoCloseable {

    @Override
    void close();

    RqlTopLevelQuery.DB db(Object... args);

    RqlTopLevelQuery.DbCreate db_create(Object... args);

    RqlTopLevelQuery.DbDrop db_drop(Object... args);

    RqlTopLevelQuery.DbList db_list(Object... args);

    RqlQuery.Table table(Object... args);
    
    RqlCursor execute(RqlQuery query);
}
