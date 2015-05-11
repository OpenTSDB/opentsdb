package net.opentsdb.uid;

/**
 * Implementations of this class should be published to a
 * {@link com.google.common.eventbus.EventBus} on an implementation specified
 * event.
 */
public class LabelEvent {
  private final byte[] id;
  private final String name;
  private final UniqueIdType type;

  /**
   * Create an event for the label with the specified arguments. No arguments
   * should be {@code null}.
   *
   * @param id   The id of the label that has had an event
   * @param name The name of the label that has had an event
   * @param type The type of the label that has had an event
   */
  public LabelEvent(final byte[] id,
                    final String name,
                    final UniqueIdType type) {
    this.id = id;
    this.name = name;
    this.type = type;
  }

  /** The ID of the label that has had an event */
  public byte[] getId() {
    return id;
  }

  /** The name of the label that has had an event */
  public String getName() {
    return name;
  }

  /** The type of the label that has had an event */
  public UniqueIdType getType() {
    return type;
  }
}
