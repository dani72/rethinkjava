package com.dkhenry.RethinkDB.errors;

public class RqlException extends RuntimeException {

    public RqlException(String message) {
        super(message);
    }
    
    public RqlException( String message, Throwable cause) {
        super( message, cause);
    }
}
