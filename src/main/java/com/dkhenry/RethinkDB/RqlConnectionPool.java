/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dkhenry.RethinkDB;

import com.dkhenry.RethinkDB.errors.RqlDriverException;
import com.dkhenry.RethinkDB.errors.RqlException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author dani
 */
public class RqlConnectionPool {

    private final AtomicInteger _count = new AtomicInteger( 0);
    private final int _capacity;
    private final ArrayBlockingQueue<PooledRqlConnection> _queue;
    private final RqlConnectionFactory _factory;

    private RqlConnectionPool( int capacity, RqlConnectionFactory factory) {
        _capacity = capacity;
        _queue = new ArrayBlockingQueue<>(capacity);
        _factory = factory;
    }
    
    public RqlConnectionPool(int capacity, String host, int port) {
        this( capacity, new SimpleRqlConnectionFactory( host, port));
    }
    
    public RqlConnectionPool( int capacity, String targets) {
        this( capacity, new BalancingRqlConnectionFactory( targets));
    }

    private PooledRqlConnection createConnection() {
        _count.incrementAndGet();
        
        return new PooledRqlConnection( _factory.create());
    }
    
    public RqlConnection get() {
        try {
            RqlConnection connection = _queue.poll();
            
            if( connection == null) {
                int c = _count.get();
                
                if( c >= _capacity) {
                    return _queue.take();
                }
                else {
                    return createConnection();
                }
            }
            else {
                return connection;
            }
        } 
        catch (InterruptedException e) {
            throw new RqlException("Could not get connection from pool.", e);
        }
    }

    private void push( PooledRqlConnection connection) {
        try {
            _queue.put(connection);
        } 
        catch (InterruptedException e) {
            throw new RqlException("Could not put back connection into pool.", e);
        }
    }

    public void shutdown() {
        PooledRqlConnection connection = _queue.poll();

        while (connection != null) {
            connection.unwrap().close();

            connection = _queue.poll();
        }
    }
    
    private interface RqlConnectionFactory {
        RqlConnection create();
    }
    
    private static class SimpleRqlConnectionFactory implements RqlConnectionFactory {

        private final String _hostname;
        private final int _port;
        
        public SimpleRqlConnectionFactory( String host, int port) {
            _hostname = host;
            _port = port;
        }
        
        @Override
        public RqlConnection create() {
            return RethinkDB.connect(_hostname, _port);
        }
    }
    
    private static class BalancingRqlConnectionFactory implements RqlConnectionFactory {
        private final List<SimpleRqlConnectionFactory> _factories = new ArrayList<>();
        private int _next = 0;

        public BalancingRqlConnectionFactory( String targets) {
            StringTokenizer tokenizer = new StringTokenizer( targets, ",");

            while( tokenizer.hasMoreTokens()) {
                String[] vals = tokenizer.nextToken().split( ":");

                _factories.add( new SimpleRqlConnectionFactory( vals[0], Integer.parseInt(vals[1])));
            }
        }
        
        @Override
        public RqlConnection create() {
            _next = (_next + 1) % _factories.size();
            
            return _factories.get( _next).create();
        }
    }
    
    private class PooledRqlConnection implements RqlConnection {
        
        private final RqlConnection _connection;
        
        public PooledRqlConnection( RqlConnection connection) {
            _connection = connection;
        }

        @Override
        public void close() {
            RqlConnectionPool.this.push( this);
        }

        public RqlConnection unwrap() {
            return _connection;
        }
        
        @Override
        public RqlTopLevelQuery.DB db(Object... args) {
            return _connection.db( args);
        }

        @Override
        public RqlTopLevelQuery.DbCreate db_create(Object... args) {
            return _connection.db_create( args);
        }

        @Override
        public RqlTopLevelQuery.DbDrop db_drop(Object... args) {
            return _connection.db_drop( args);
        }

        @Override
        public RqlTopLevelQuery.DbList db_list(Object... args) {
            return _connection.db_list( args);
        }

        @Override
        public RqlQuery.Table table(Object... args) {
            return _connection.table( args);
        }

        @Override
        public RqlCursor run(RqlQuery query) {
            return _connection.run( query);
        }
    }
};
