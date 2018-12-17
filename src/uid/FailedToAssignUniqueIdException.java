package net.opentsdb.uid;

/**
 * Thrown when we failed to assign an ID to a string such as a metric, tagk
 * or tag v.
 * @see UniqueId
 */
public final class FailedToAssignUniqueIdException extends RuntimeException {
  /** The 'kind' of the table. */
  private final String kind;
  /** The name of the object attempting to be assigned */
  private final String name;
  /** How many attempts were made to assign the ID */
  private final int attempts;
  
  /**
   * CTor
   * @param kind The kind of object that couldn't be assigned
   * @param name The name of the object that couldn't be assigned
   * @param attempts How many attempts were made to assign
   */
  public FailedToAssignUniqueIdException(final String kind, final String name, 
      final int attempts) {
    super("Failed to assign random ID for kind='" + kind + "' name='" + 
      name + "' after " + attempts + " attempts");
    this.kind = kind;
    this.name = name;
    this.attempts = attempts;
  }

  /**
   * CTor
   * @param kind The kind of object that couldn't be assigned
   * @param name The name of the object that couldn't be assigned
   * @param attempts How many attempts were made to assign
   * @param msg A message to append
   * @since 2.3
   */
  public FailedToAssignUniqueIdException(final String kind, final String name, 
      final int attempts, final String msg) {
    super("Failed to assign ID for kind='" + kind + "' name='" + 
      name + "' after " + attempts + " attempts due to: " + msg);
    this.kind = kind;
    this.name = name;
    this.attempts = attempts;
  }
  
  /**
   * CTor
   * @param kind The kind of object that couldn't be assigned
   * @param name The name of the object that couldn't be assigned
   * @param attempts How many attempts were made to assign
   * @param ex An exception that caused assignment to fail
   */
  public FailedToAssignUniqueIdException(final String kind, final String name, 
      final int attempts, final Throwable ex) {
    super("Failed to assign random ID for kind='" + kind + "' name='" + 
      name + "' after " + attempts + " attempts", ex);
    this.kind = kind;
    this.name = name;
    this.attempts = attempts;
  }
  
  
  /** @return Returns the kind of unique ID that couldn't be assigned.  */
  public String kind() {
    return kind;
  }

  /** @return Returns the name of the object that couldn't be assigned */
  public String name() {
    return name;
  }
  
  /** @return Returns how many attempts were made to assign a UID */
  public int attempts() {
    return attempts;
  }
  
  private static final long serialVersionUID = 399163221436118367L;

}
