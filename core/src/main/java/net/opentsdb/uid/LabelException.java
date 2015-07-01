package net.opentsdb.uid;

/**
 * A specialized exception that is thrown when something is wrong with an ID or name.
 */
public class LabelException extends Exception {
  public LabelException(final String name,
                        final LabelType type,
                        final String message) {
    super(name + " (" + type.toValue() + "): " + message);
  }

  public LabelException(final long id,
                        final LabelType type,
                        final String message) {
    super(id + " (" + type.toValue() + "): " + message);
  }
}
