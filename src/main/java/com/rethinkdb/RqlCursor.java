package com.rethinkdb;

import com.rethinkdb.impl.RqlObject;
import java.util.Iterator;

/**
 *
 * @author dani
 */
public interface RqlCursor extends Iterable<RqlObject>, Iterator<RqlObject> {

    @Override
    boolean hasNext();

    @Override
    Iterator<RqlObject> iterator();

    @Override
    RqlObject next();
}
