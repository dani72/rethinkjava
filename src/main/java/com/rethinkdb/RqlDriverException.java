package com.rethinkdb;

public class RqlDriverException extends RuntimeException {

    public RqlDriverException(String message) {
        super(message);
    }
    
    public RqlDriverException( String message, Throwable cause) {
        super( message, cause);
    }
}
