package com.rethinkdb;

public class RqlException extends RuntimeException {

    public RqlException(String message) {
        super(message);
    }
    
    public RqlException( String message, Throwable cause) {
        super( message, cause);
    }
}
