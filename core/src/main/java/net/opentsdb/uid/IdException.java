package net.opentsdb.uid;

/**
 * A specialized exception that is thrown when something is wrong with an ID or name.
 */
public class IdException extends Exception {
  public IdException(final String name,
                     final UniqueIdType type,
                     final String message) {
    super(name + " (" + type.toValue() + "): " + message);
  }

  public IdException(final long id,
                     final UniqueIdType type,
                     final String message) {
    super(id + " (" + type.toValue() + "): " + message);
  }
}
