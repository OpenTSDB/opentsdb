package net.opentsdb.query;

public class IncompleteStateException extends RuntimeException {

    public IncompleteStateException() {
        super();
    }

    public IncompleteStateException(String message) {
        super(message);
    }

    public IncompleteStateException(String message, Throwable t) {
        super(message, t);
    }

    public IncompleteStateException(Throwable t) {
        super(t);
    }

}
